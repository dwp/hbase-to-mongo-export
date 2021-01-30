package app.services.impl

import app.services.SnsService
import app.utils.PropertyUtility
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.model.PublishRequest
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Component
class SnsServiceImpl(private val sns: AmazonSNS): SnsService {

    override fun sendExportCompletedMessage() = sendMessage(targetTopicArn(), exportCompletedPayload())
    override fun sendMonitoringMessage() = sendMessage(monitoringTopicArn, monitoringPayload())

    private fun sendMessage(topicArn: String, payload: String) {
        topicArn.takeIf(String::isNotBlank)?.let { arn ->
            logger.info("Publishing message to topic", "arn" to arn)
            val result = sns.publish(request(arn, payload))
            logger.info("Published message to adg trigger topic", "arn" to arn,
                "message_id" to result.messageId, "snapshot_type" to snapshotType)
        } ?: run {
            logger.info("Not publishing message to topic", "reason" to "No arn configured")
        }
    }

    private fun exportCompletedPayload() =
            """{
                "correlation_id": "${PropertyUtility.correlationId()}",
                "s3_prefix": "$s3prefix"   
            }"""

    private fun monitoringPayload() =
            """{
                "severity": "Critical",
                "notification_type": "Information",
                "slack_username": "Crown Export Poller",
                "title_text": "$snapshotType - Export finished",
            }"""

    private fun request(arn: String, payload: String) =
        PublishRequest().apply {
            topicArn = arn
            message = payload
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

    @Value("\${monitoring.topic.arn:}")
    private lateinit var monitoringTopicArn: String

    @Value("\${snapshot.type}")
    private lateinit var snapshotType: String

    @Value("\${s3.prefix.folder}")
    private lateinit var s3prefix: String

    companion object {
        private val logger = DataworksLogger.getLogger(SnsServiceImpl::class)
    }
}
