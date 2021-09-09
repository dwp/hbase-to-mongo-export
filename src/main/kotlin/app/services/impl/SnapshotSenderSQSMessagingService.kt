package app.services.impl

import app.services.SnapshotSenderMessagingService
import app.utils.PropertyUtility.correlationId
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model.SendMessageRequest
import org.springframework.beans.factory.annotation.Value
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
class SnapshotSenderSQSMessagingService(private val amazonSQS: AmazonSQS) : SnapshotSenderMessagingService {

    @Retryable(value = [Exception::class],
            maxAttemptsExpression = "\${sqs.retry.maxAttempts:5}",
            backoff = Backoff(delayExpression = "\${sqs.retry.delay:1000}",
                    multiplierExpression = "\${sqs.retry.multiplier:2}"))
    override fun notifySnapshotSender(prefix: String) {
        if (triggerSnapshotSender.toBoolean()) {
            amazonSQS.sendMessage(sendMessageRequest(message(prefix), snapshotSenderSqsQueueUrl))
            logger.info("Sent message to snapshot sender queue", "prefix" to prefix)
        }
    }

    @Retryable(value = [Exception::class],
        maxAttemptsExpression = "\${sqs.retry.maxAttempts:5}",
        backoff = Backoff(delayExpression = "\${sqs.retry.delay:1000}",
            multiplierExpression = "\${sqs.retry.multiplier:2}"))
    override fun notifySnapshotSenderNoFilesExported() {
        if (triggerSnapshotSender.toBoolean()) {
            amazonSQS.sendMessage(sendMessageRequest(noFilesExportedMessage(), snapshotSenderSqsQueueUrl))
            logger.info("Sent no files exported message to snapshot sender queue")
        }
    }

    @Retryable(value = [Exception::class],
            maxAttemptsExpression = "\${sqs.retry.maxAttempts:5}",
            backoff = Backoff(delayExpression = "\${sqs.retry.delay:1000}",
                    multiplierExpression = "\${sqs.retry.multiplier:2}"))
    override fun sendDataEgressMessage(prefix: String) {
        logger.info("Sending message to data egress queue")
        val message = dataEgressRisMessage(prefix)
        amazonSQS.sendMessage(sendMessageRequest(message, dataEgressSqsQueueUrl) )
        logger.info("Sent message to data egress queue", "message" to message)
    }

    private fun sendMessageRequest(message: String, sqsQueueUrl: String) =
            SendMessageRequest().apply {
                queueUrl = sqsQueueUrl
                messageBody = message
                messageGroupId = topicName.replace(".", "_")
            }

    private fun message(prefix: String)= """
            |{
            |   "shutdown_flag": "$shutdown",
            |   "correlation_id": "${correlationId()}",
            |   "topic_name": "$topicName",
            |   "export_date": "$exportDate",
            |   "reprocess_files": "$reprocess",
            |   "s3_full_folder": "$prefix",
            |   "snapshot_type": "$snapshotType"
            |}
            """.trimMargin()

    private fun noFilesExportedMessage() = """
            |{
            |   "shutdown_flag": "$shutdown",
            |   "correlation_id": "${correlationId()}",
            |   "topic_name": "$topicName",
            |   "export_date": "$exportDate",
            |   "reprocess_files": "$reprocess",
            |   "snapshot_type": "$snapshotType",
            |   "files_exported": 0
            |}
            """.trimMargin()


    private fun dataEgressRisMessage(key: String) = """
            |{
            |   "Records: [
            |   {
            |       "s3": {
            |           "object": {
            |               "key": "$key"
            |           }
            |       }
            |   }
            |  ]
            |}
            """.trimMargin()

    private val reprocess by lazy { reprocessFiles.toBoolean() }
    private val shutdown by lazy { shutdownOnCompletion.toBoolean() }

    @Value("\${topic.name}")
    private lateinit var topicName: String

    @Value("\${snapshot.sender.sqs.queue.url}")
    private lateinit var snapshotSenderSqsQueueUrl: String

    @Value("\${data.egress.sqs.queue.url}")
    private lateinit var dataEgressSqsQueueUrl: String

    @Value("\${snapshot.sender.reprocess.files}")
    private lateinit var reprocessFiles: String

    @Value("\${snapshot.sender.shutdown.flag}")
    private lateinit var shutdownOnCompletion: String

    @Value("\${snapshot.sender.export.date}")
    private lateinit var exportDate: String

    @Value("\${trigger.snapshot.sender}")
    private lateinit var triggerSnapshotSender: String

    @Value("\${snapshot.type}")
    private lateinit var snapshotType: String

    companion object {
        val logger = DataworksLogger.getLogger(SnapshotSenderSQSMessagingService::class)
    }
}
