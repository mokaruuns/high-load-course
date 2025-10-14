package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.*
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )
    private val window = Duration.ofSeconds(1)

    private val hardCap1s = FixedWindowRateLimiter(11, 1, TimeUnit.SECONDS)

    private val swRL = LeakingBucketRateLimiter(
        rate = 11,
        window = Duration.ofSeconds(1),
        bucketSize = 270
    )
    private val limiterEpochMs = System.currentTimeMillis()
    private val windowMs = window.toMillis()

    private fun msUntilNextDrain(now: Long): Long {
        val elapsed = now - limiterEpochMs
        val rem = elapsed % windowMs
        return if (rem == 0L) windowMs else (windowMs - rem)
    }

    private fun msUntilNextSecond(now: Long): Long =
        (1000L - (now % 1000L)).let { if (it == 0L) 1000L else it }

    class PaymentRejectedException(val estimatedCompletionTimestamp: Long) :
        RuntimeException("Payment rejected due to high load. Retry after $estimatedCompletionTimestamp timestamp.") {
        override fun fillInStackTrace(): Throwable = this
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        val hardCapOk = hardCap1s.tick()
        val bucketOk  = if (hardCapOk) swRL.tick() else false

        if (!hardCapOk || !bucketOk) {
            val retryAfterMs = maxOf(msUntilNextSecond(createdAt), msUntilNextDrain(createdAt))
            throw PaymentRejectedException(createdAt + retryAfterMs)
        }

        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }
}