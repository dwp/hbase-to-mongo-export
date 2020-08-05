package app.services.impl

import app.services.SnapshotSenderMessagingService
import app.utils.logging.logInfo
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model.SendMessageRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service

@Service
class SnapshotSenderSQSMessagingService(private val amazonSQS: AmazonSQS) : SnapshotSenderMessagingService {

    @Retryable(value = [Exception::class],
            maxAttempts = maxAttempts,
            backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    override fun notifySnapshotSender(prefix: String) {
        if (triggerSnapshotSender.toBoolean()) {
            amazonSQS.sendMessage(sendMessageRequest(message(prefix)))
            logInfo(logger, "Sent message to snapshot sender queue", "prefix", prefix)
        }
    }

    private fun sendMessageRequest(message: String) =
            SendMessageRequest().apply {
                queueUrl = sqsQueueUrl
                messageBody = message
                delaySeconds = messageDelaySeconds.toInt()
            }

    private fun message(prefix: String)= """
            |{
            |   "shutdown_flag": "$shutdown",
            |   "correlation_id": "$correlationId",
            |   "topic_name": "$topicName",
            |   "export_date": "$exportDate",
            |   "reprocess_files": "$reprocess",
            |   "s3_full_folder": "$prefix",
            |   "snapshot_type": "$snapshotType"
            |}
            """.trimMargin()

    private val reprocess by lazy { reprocessFiles.toBoolean() }
    private val shutdown by lazy { shutdownOnCompletion.toBoolean() }
    private val correlationId by lazy { System.getProperty("correlation_id") }

    @Value("\${topic.name}")
    private lateinit var topicName: String

    @Value("\${snapshot.sender.sqs.queue.url}")
    private lateinit var sqsQueueUrl: String

    @Value("\${snapshot.sender.reprocess.files}")
    private lateinit var reprocessFiles: String

    @Value("\${snapshot.sender.shutdown.flag}")
    private lateinit var shutdownOnCompletion: String

    @Value("\${snapshot.sender.export.date}")
    private lateinit var exportDate: String

    @Value("\${trigger.snapshot.sender}")
    private lateinit var triggerSnapshotSender: String

    @Value("\${message.delay.seconds:30}")
    private lateinit var messageDelaySeconds: String

    @Value("\${snapshot.type}")
    private lateinit var snapshotType: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(SnapshotSenderSQSMessagingService::class.toString())
        const val maxAttempts = 5
        const val initialBackoffMillis = 1000L
        const val backoffMultiplier = 2.0
    }
}
