package app.services.impl

import app.services.SnsService
import app.utils.PropertyUtility
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.model.PublishRequest
import com.amazonaws.services.sns.model.PublishResult
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Component
class SnsServiceImpl(private val sns: AmazonSNS): SnsService {

    override fun sendExportCompletedMessage() {
        targetTopicArn().takeIf(String::isNotBlank)?.let { arn ->
            val result: PublishResult = sns.publish(PublishRequest().apply {
                topicArn = arn
                message = """{
                    "correlation_id": "${PropertyUtility.correlationId()}",
                    "s3_prefix": "$s3prefix"   
                }""".trimIndent()
            })
            logger.info("Published message to adg trigger topic", "arn" to arn,
                "message_id" to result.messageId, "snapshot_type" to snapshotType)
        } ?: run {
            logger.info("Not publishing message to adg trigger topic", "reason" to "No arn configured")
        }
    }

    private fun targetTopicArn(): String =
        when (snapshotType) {
            "full" -> {
                fullTopicArn
            }
            else -> {
                incrementalTopicArn
            }
        }

    @Value("\${full.topic.arn:}")
    private lateinit var fullTopicArn: String

    @Value("\${incremental.topic.arn:}")
    private lateinit var incrementalTopicArn: String

    @Value("\${snapshot.type}")
    private lateinit var snapshotType: String

    @Value("\${s3.prefix.folder}")
    private lateinit var s3prefix: String

    companion object {
        private val logger = DataworksLogger.getLogger(SnsServiceImpl::class)
    }
}
