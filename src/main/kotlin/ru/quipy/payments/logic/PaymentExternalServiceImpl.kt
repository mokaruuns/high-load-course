package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


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
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val parallelRequestsLimiter = OngoingWindow(parallelRequests)
    private val client = OkHttpClient.Builder()
        .callTimeout(Duration.ofMillis(requestAverageProcessingTime.toMillis() * 2))
        .build()

    private val maxRetries = 3


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(
                success = true, transactionId, now(),
                Duration.ofMillis(now() - paymentStartedAt)
            )
        }
        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var attempt = 0
        while (true) {
            attempt++

            try {
                // быстрая проверка смысла очередной попытки
                val guardMs = properties.averageProcessingTime.toMillis()
                val remainingMs = deadline - now()
                if (remainingMs <= guardMs) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Stopped before deadline")
                    }
                    logger.warn("[$accountName] No time left before deadline, stop retries. txId=$transactionId")
                    return
                }

                // лимитируем только на время вызова
                parallelRequestsLimiter.acquire()
                rateLimiter.tickBlocking()

                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                client.newCall(request).execute().use { response ->
                    val code = response.code
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Parse error, code=$code", e)
                        ExternalSysResponse(
                            transactionId.toString(),
                            paymentId.toString(),
                            false,
                            "parse_error:${e.message}"
                        )
                    }
                    logger.warn("[$accountName] Parse error, code=$code, body=$body, respone=$response, transactionId=$transactionId")

                    val success = response.isSuccessful && body.result
                    paymentESService.update(paymentId) {
                        it.logProcessing(success, now(), transactionId, reason = body.message)
                    }

                    if (success) {
                        logger.warn("[$accountName] Success on attempt $attempt, txId=$transactionId")
                        return
                    }

                    val retryableHttp = (code == 429) || (code in 500..599)
                    if (!retryableHttp || attempt > maxRetries) {
                        logger.warn("[$accountName] Finish without retry. http=$code, attempt=$attempt, txId=$transactionId")
                        return
                    }
                }
            } catch (e: Exception) {
                val retryableEx = (e is SocketTimeoutException) || (e is java.io.IOException)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message ?: e::class.java.simpleName)
                }
                if (!retryableEx || attempt > maxRetries) {
                    logger.error(
                        "[$accountName] Payment failed (no more retries) on attempt $attempt, txId=$transactionId",
                        e
                    )
                    return
                }
                logger.warn("[$accountName] Retryable exception (${e::class.java.simpleName}) on attempt $attempt")
            } finally {
                parallelRequestsLimiter.release()
            }

            val guardMs = properties.averageProcessingTime.toMillis()
            val remainingMs = deadline - now()
            if (remainingMs <= guardMs) {
                paymentESService.update(paymentId) {
                    it.logProcessing(
                        false,
                        now(),
                        transactionId,
                        reason = "Stopped before deadline (no time for backoff)"
                    )
                }
                logger.warn("[$accountName] No time for backoff, stop. txId=$transactionId")
                return
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()