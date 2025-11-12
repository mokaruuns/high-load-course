package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletionException
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

        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val requestTimeout = requestAverageProcessingTime.multipliedBy(2)
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val parallelRequestsLimiter = OngoingWindow(parallelRequests)
    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(requestAverageProcessingTime)
        .build()

    private val maxRetries = 3
    private val maxAttempts = maxRetries + 1


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

        submitAttempt(paymentId, amount, paymentStartedAt, deadline, transactionId, attempt = 1)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun submitAttempt(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        transactionId: UUID,
        attempt: Int,
    ) {
        if (!hasTimeForAttempt(deadline)) {
            recordDeadlineStop(paymentId, transactionId, "Stopped before deadline")
            logger.warn("[$accountName] No time left before deadline, stop retries. txId=$transactionId")
            return
        }

        try {
            parallelRequestsLimiter.acquire()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            logger.warn("[$accountName] Interrupted while acquiring slot, txId=$transactionId")
            return
        }

        try {
            rateLimiter.tickBlocking()
            val request = HttpRequest.newBuilder()
                .uri(buildUri(transactionId, paymentId, amount))
                .timeout(requestTimeout)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()

            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .orTimeout(requestTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete { response, throwable ->
                    try {
                        handleCompletion(
                            response,
                            throwable,
                            paymentId,
                            amount,
                            paymentStartedAt,
                            deadline,
                            transactionId,
                            attempt
                        )
                    } finally {
                        parallelRequestsLimiter.release()
                    }
                }
        } catch (e: Exception) {
            parallelRequestsLimiter.release()
            handleAttemptException(
                paymentId,
                transactionId,
                attempt,
                e,
                amount,
                paymentStartedAt,
                deadline
            )
        }
    }

    private fun handleCompletion(
        response: HttpResponse<String>?,
        throwable: Throwable?,
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        transactionId: UUID,
        attempt: Int,
    ) {
        if (throwable != null) {
            val actual = unwrap(throwable)
            handleAttemptException(
                paymentId,
                transactionId,
                attempt,
                actual,
                amount,
                paymentStartedAt,
                deadline
            )
            return
        }

        if (response == null) {
            handleAttemptException(
                paymentId,
                transactionId,
                attempt,
                IllegalStateException("Null response"),
                amount,
                paymentStartedAt,
                deadline
            )
            return
        }

        val code = response.statusCode()
        val body = parseBody(response.body(), paymentId, transactionId, code)
        val success = code in 200..299 && body.result

        paymentESService.update(paymentId) {
            it.logProcessing(success, now(), transactionId, reason = body.message)
        }

        if (success) {
            logger.warn("[$accountName] Success on attempt $attempt, txId=$transactionId")
            return
        }

        logger.warn("[$accountName] Finish attempt $attempt with http=$code, txId=$transactionId, body=$body")
        if (shouldRetry(attempt)) {
            retryOrStop(paymentId, amount, paymentStartedAt, deadline, transactionId, attempt)
        } else {
            logger.warn("[$accountName] Finish without retry. http=$code, attempt=$attempt, txId=$transactionId")
        }
    }

    private fun handleAttemptException(
        paymentId: UUID,
        transactionId: UUID,
        attempt: Int,
        throwable: Throwable,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
    ) {
        paymentESService.update(paymentId) {
            it.logProcessing(
                false,
                now(),
                transactionId,
                reason = throwable.message ?: throwable::class.java.simpleName
            )
        }

        val retryable = isRetryable(throwable)
        if (!retryable || !shouldRetry(attempt)) {
            logger.error(
                "[$accountName] Payment failed (no more retries) on attempt $attempt, txId=$transactionId",
                throwable
            )
            return
        }

        logger.warn("[$accountName] Retryable exception (${throwable::class.java.simpleName}) on attempt $attempt")
        retryOrStop(paymentId, amount, paymentStartedAt, deadline, transactionId, attempt)
    }

    private fun retryOrStop(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        transactionId: UUID,
        previousAttempt: Int,
    ) {
        if (!hasTimeForAttempt(deadline)) {
            recordDeadlineStop(paymentId, transactionId, "Stopped before deadline (no time for backoff)")
            logger.warn("[$accountName] No time for backoff, stop. txId=$transactionId")
            return
        }

        submitAttempt(
            paymentId,
            amount,
            paymentStartedAt,
            deadline,
            transactionId,
            previousAttempt + 1
        )
    }

    private fun parseBody(
        body: String?,
        paymentId: UUID,
        transactionId: UUID,
        code: Int,
    ): ExternalSysResponse {
        return try {
            mapper.readValue(body, ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] Parse error, code=$code, txId=$transactionId", e)
            ExternalSysResponse(
                transactionId.toString(),
                paymentId.toString(),
                false,
                "parse_error:${e.message}"
            )
        }
    }

    private fun hasTimeForAttempt(deadline: Long): Boolean {
        val guardMs = properties.averageProcessingTime.toMillis()
        val remainingMs = deadline - now()
        return remainingMs > guardMs
    }

    private fun recordDeadlineStop(paymentId: UUID, transactionId: UUID, reason: String) {
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    private fun buildUri(transactionId: UUID, paymentId: UUID, amount: Int): URI {
        return URI.create(
            "http://$paymentProviderHostPort/external/process" +
                "?serviceName=$serviceName" +
                "&token=$token" +
                "&accountName=$accountName" +
                "&transactionId=$transactionId" +
                "&paymentId=$paymentId" +
                "&amount=$amount"
        )
    }

    private fun shouldRetry(attempt: Int) = attempt < maxAttempts

    private fun unwrap(error: Throwable): Throwable {
        return if (error is CompletionException && error.cause != null) {
            error.cause!!
        } else {
            error
        }
    }

    private fun isRetryable(throwable: Throwable): Boolean {
        val actual = if (throwable is CompletionException && throwable.cause != null) throwable.cause!! else throwable
        return actual is IOException ||
            actual is SocketTimeoutException ||
            actual is HttpTimeoutException
    }
}

public fun now() = System.currentTimeMillis()
