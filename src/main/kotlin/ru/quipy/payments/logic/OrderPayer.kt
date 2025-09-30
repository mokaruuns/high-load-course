package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.time.Duration
import kotlin.math.min

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
        110,
        110,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    class PaymentRejectedException(val estimatedCompletionTimestamp: Long) :
        RuntimeException("Payment rejected due to high load. Retry after $estimatedCompletionTimestamp timestamp.") {
        override fun fillInStackTrace(): Throwable = this
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        val currQueueSize = paymentExecutor.queue.size

//        PaymentAccountProperties(serviceName=marsel-repo, accountName=acc-18, parallelRequests=10000, rateLimitPerSec=110, price=30, averageProcessingTime=PT1S, enabled=true)
        val parallelRequests = 10000
        val rateLimitPerSec = 110
        val requestAverageProcessingTime = Duration.ofSeconds(1)

        val parallelSpeed = parallelRequests * 1000 / requestAverageProcessingTime.toMillis()
        val speed = min(parallelSpeed, rateLimitPerSec.toLong())
        val estimatedWaitTimeInSeconds = currQueueSize.toDouble() / speed
        val estimatedWaitTime = (estimatedWaitTimeInSeconds * 1000).toLong()
        val estimatedCompletionTime = createdAt + estimatedWaitTime

        logger.warn("paymentId: {} estimatedWaitTimeInSeconds: {} sec currQueueSize: {}", paymentId, estimatedWaitTimeInSeconds, currQueueSize)
        if (estimatedCompletionTime > deadline) {
            throw PaymentRejectedException(estimatedCompletionTime)
        }
        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.warn("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }
}