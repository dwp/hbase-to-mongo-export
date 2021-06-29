package app.services.impl

import app.services.SnsService
import app.utils.PropertyUtility.correlationId
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.model.PublishRequest
import org.springframework.beans.factory.annotation.Value
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger
import org.springframework.batch.core.ExitStatus

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
    override fun sendMonitoringMessage(exitStatus: ExitStatus) =
        sendMessage(monitoringTopicArn, monitoringPayload(exitStatus))

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
            if (skipPdmTrigger.isNotBlank() && skipPdmTrigger != "NOT_SET")
                """{
                    "correlation_id": "${correlationId()}",
                    "s3_prefix": "$s3prefix",
                    "snapshot_type": "$snapshotType",
                    "export_date": "$exportDate",
                    "skip_pdm_trigger": "$skipPdmTrigger"
                }"""
            else
                """{
                    "correlation_id": "${correlationId()}",
                    "s3_prefix": "$s3prefix",
                    "snapshot_type": "$snapshotType",
                    "export_date": "$exportDate"
                }"""

    private fun monitoringPayload(exitStatus: ExitStatus) =
            """{
                "severity": "${severity(exitStatus)}",
                "notification_type": "${notificationType(exitStatus)}",
                "slack_username": "HTME",
                "title_text": "${snapshotType.capitalize()} - Export finished - ${exitStatus.toString()}",
                "custom_elements": [
                    {
                        "key": "Export date",
                        "value": "$exportDate"
                    },
                    {
                        "key": "Correlation Id",
                        "value": "${correlationId()}"
                    },
                    {
                        "key": "Topic",
                        "value": "$topicName"
                    }
                ]
            }"""

    private fun severity(exitStatus: ExitStatus): String =
        when (exitStatus) {
            ExitStatus.COMPLETED -> {
                "Critical"
            }
            else -> {
                "High"
            }
        }

    private fun notificationType(exitStatus: ExitStatus): String =
        when (exitStatus) {
            ExitStatus.COMPLETED -> {
                "Information"
            }
            else -> {
                "Warning"
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

    @Value("\${skip.pdm.trigger}")
    private lateinit var skipPdmTrigger: String

    @Value("\${topic.name}")
    private lateinit var topicName: String

    companion object {
        private val logger = DataworksLogger.getLogger(SnsServiceImpl::class)
    }
}
