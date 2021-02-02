package app.services.impl

import app.services.ExportCompletionStatus
import app.services.SnsService
import app.utils.PropertyUtility
import app.utils.PropertyUtility.correlationId
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.model.PublishRequest
import org.springframework.beans.factory.annotation.Value
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Component
class SnsServiceImpl(private val sns: AmazonSNS): SnsService {

    @Retryable(value = [Exception::class],
        maxAttemptsExpression = "\${sns.retry.maxAttempts:5}",
        backoff = Backoff(delayExpression = "\${sns.retry.delay:1000}",
            multiplierExpression = "\${sns.retry.multiplier:2}"))
    override fun sendExportCompletedSuccessfullyMessage() =
        sendMessage(targetTopicArn(), exportCompletedPayload())

    @Retryable(value = [Exception::class],
        maxAttemptsExpression = "\${sns.retry.maxAttempts:5}",
        backoff = Backoff(delayExpression = "\${sns.retry.delay:1000}",
            multiplierExpression = "\${sns.retry.multiplier:2}"))
    override fun sendMonitoringMessage(completionStatus: ExportCompletionStatus) =
        sendMessage(monitoringTopicArn, monitoringPayload(completionStatus))

    private fun sendMessage(topicArn: String, payload: String) {
        topicArn.takeIf(String::isNotBlank)?.let { arn ->
            logger.info("Publishing message to topic", "arn" to arn)
            val result = sns.publish(request(arn, payload))
            logger.info("Published message to topic", "arn" to arn,
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

    private fun monitoringPayload(exportCompletionStatus: ExportCompletionStatus) =
            """{
                "severity": ${severity(exportCompletionStatus)},
                "notification_type": ${notificationType(exportCompletionStatus)},
                "slack_username": "Crown Export Poller",
                "title_text": "${snapshotType.capitalize()} - Export finished - ${exportCompletionStatus.description}",
                "custom_elements": [
                    {
                        "key": "Export date",
                        "value": "$exportDate"
                    },
                    {
                        "key": "Correlation Id",
                        "value": "${correlationId()}"
                    }
                ]
            }"""

    private fun severity(exportCompletionStatus: ExportCompletionStatus): String =
        when (exportCompletionStatus) {
            ExportCompletionStatus.COMPLETED_SUCCESSFULLY -> {
                "Critical"
            }
            else -> {
                "High"
            }
        }

    private fun notificationType(exportCompletionStatus: ExportCompletionStatus): String =
        when (exportCompletionStatus) {
            ExportCompletionStatus.COMPLETED_UNSUCCESSFULLY -> {
                "Error"
            }
            else -> {
                "Information"
            }
        }

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

    @Value("\${topic.arn.completion.full:}")
    private lateinit var fullTopicArn: String

    @Value("\${topic.arn.completion.incremental:}")
    private lateinit var incrementalTopicArn: String

    @Value("\${topic.arn.monitoring:}")
    private lateinit var monitoringTopicArn: String

    @Value("\${snapshot.type}")
    private lateinit var snapshotType: String

    @Value("\${snapshot.sender.export.date}")
    private lateinit var exportDate: String

    @Value("\${s3.prefix.folder}")
    private lateinit var s3prefix: String

    companion object {
        private val logger = DataworksLogger.getLogger(SnsServiceImpl::class)
    }
}
