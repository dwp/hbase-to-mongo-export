package app.services.impl

import app.services.SnapshotSenderMessagingService
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model.SendMessageRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service


@Service
class SnapshotSenderSQSMessagingService(private val amazonSQS: AmazonSQS) : SnapshotSenderMessagingService {

    override fun notifySnapshotSender(prefix: String) {
        amazonSQS.sendMessage(sendMessageRequest(message(prefix)))
        logger.info("Sent message to snapshot sender queue", "prefix", prefix)
    }

    private fun sendMessageRequest(message: String) =
            SendMessageRequest().apply {
            queueUrl = sqsQueueUrl
            messageBody = message
        }

    private fun message(prefix: String): String {
        val message = """
            |{
            |   "shutdown_flag": "$shutdown",
            |   "correlation_id": "$correlationId",
            |   "topic_name": "$topicName",
            |   "export_date": "$exportDate",
            |   "reprocess_files": "$reprocess",
            |   "s3_full_folder": "$prefix"
            |}
            """.trimMargin()
        return message
    }

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

    companion object {
        val logger: Logger = LoggerFactory.getLogger(SnapshotSenderSQSMessagingService::class.toString())
    }
}

