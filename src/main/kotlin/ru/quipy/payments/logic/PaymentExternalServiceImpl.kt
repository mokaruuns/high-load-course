package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.FixedWindowRateLimiter
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*


class PaymentRejectedException(val retryAfter: Duration) :
    RuntimeException("Payment rejected due to high load. Retry after ${retryAfter.seconds} seconds.") {
    // Оптимизация: нам не нужен дорогой stack trace для этого типа исключений,
    // так как это ожидаемое поведение при перегрузке.
    override fun fillInStackTrace(): Throwable = this
}

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val parallelRequestsLimiter = OngoingWindow(parallelRequests)

    private val queueCapacity = 100
    private val paymentExecutor: ThreadPoolExecutor = ThreadPoolExecutor(
        parallelRequests,
        parallelRequests,
        60L, TimeUnit.SECONDS,
        LinkedBlockingQueue<Runnable>(queueCapacity)
    )


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val currentQueueSize = paymentExecutor.queue.size
        if (currentQueueSize >= queueCapacity) {
            throw PaymentRejectedException(requestAverageProcessingTime)
        }

        val estimatedWaitTime = (currentQueueSize / parallelRequests) * requestAverageProcessingTime.toMillis()
        val estimatedCompletionTime = System.currentTimeMillis() + estimatedWaitTime

        if (estimatedCompletionTime > deadline) {
            logger.warn(
                "[$accountName] Rejecting payment $paymentId due to high load. " +
                        "Queue size: $currentQueueSize, estimated wait: ${estimatedWaitTime}ms, deadline will be missed."
            )
            throw PaymentRejectedException(Duration.ofMillis(estimatedWaitTime))
        }


        paymentExecutor.submit {
            try {
                paymentExecutor.submit {
                    try {
                        executePayment(paymentId, amount, paymentStartedAt, deadline)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Unhandled exception in payment executor for $paymentId", e)
                    }
                }
            } catch (e: RejectedExecutionException) {
                // Это происходит, когда очередь заполнена. Это наш back pressure!
                logger.warn(
                    "[$accountName] Rejecting payment $paymentId because executor queue is full. " +
                            "Queue size: ${paymentExecutor.queue.size}"
                )
                // Возвращаем среднее время ожидания, так как это хороший индикатор времени на "разгрузку"
                throw PaymentRejectedException(requestAverageProcessingTime)
            }
        }
    }

    private fun executePayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            parallelRequestsLimiter.acquire()
            rateLimiter.tickBlocking()

            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            parallelRequestsLimiter.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()