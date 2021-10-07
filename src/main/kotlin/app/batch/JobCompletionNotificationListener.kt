@file:Suppress("ReplaceCallWithBinaryOperator")

package app.batch

import app.exceptions.BlockedTopicException
import app.services.*
import app.utils.TextUtils
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Summary
import org.apache.hadoop.hbase.TableNotEnabledException
import org.apache.hadoop.hbase.TableNotFoundException
import org.apache.hadoop.hbase.client.Connection
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.listener.JobExecutionListenerSupport
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger
import uk.gov.dwp.dataworks.logging.LogFields

@Component
class JobCompletionNotificationListener(
    private val exportStatusService: ExportStatusService,
    private val productStatusService: ProductStatusService,
    private val messagingService: MessagingService,
    private val snsService: SnsService,
    private val pushGatewayService: PushGatewayService,
    private val topicDurationSummary: Summary,
    private val runningApplicationsGauge: Gauge,
    private val topicsStartedCounter: Counter,
    private val topicsCompletedCounter: Counter,
    private val connection: Connection,
    private val textUtils: TextUtils,
) : JobExecutionListenerSupport() {

    override fun beforeJob(jobExecution: JobExecution) {
        LogFields.put("SNAPSHOT_TYPE", "snapshot_type", snapshotType)
        LogFields.put("TOPIC_NAME", "topic_name", topicName)
        timer
        topicsStartedCounter.inc()
        runningApplicationsGauge.inc()
        pushGatewayService.pushMetrics()
    }

    override fun afterJob(jobExecution: JobExecution) {
        try {
            logger.info("Job completed", "exit_status" to jobExecution.exitStatus.exitCode)
            setExportStatus(jobExecution)
            sendSnapshotSenderMessages(jobExecution)
            sendTopicFailedMonitoringMessage(jobExecution)

            val completionStatus = exportStatusService.exportCompletionStatus()
            sendAdgMessage(completionStatus)
            sendDataEgressRisMessage(jobExecution)
            sendPdmCommonModelMessage(completionStatus)
            setProductStatus(completionStatus)
            sendCompletionMonitoringMessage(completionStatus)
        } finally {
            runningApplicationsGauge.dec()
            topicsCompletedCounter.inc()
            val timeTaken = timer.observeDuration()
            logger.info("Time taken", "duration" to "$timeTaken")
            pushGatewayService.pushFinalMetrics()
        }
    }

    private fun setExportStatus(jobExecution: JobExecution) {
        if (jobExecution.exitStatus.equals(ExitStatus.COMPLETED)) {
            exportStatusService.setExportedStatus()
        } else {
            when {
                isATableUnavailableException(jobExecution.allFailureExceptions) -> {
                    logger.error("Setting table unavailable status",
                            "job_exit_status" to "${jobExecution.exitStatus}")
                    exportStatusService.setTableUnavailableStatus()
                }
                isABlockedTopicException(jobExecution.allFailureExceptions) -> {
                    logger.error("Setting blocked topic status",
                            "job_exit_status" to "${jobExecution.exitStatus}")
                    exportStatusService.setBlockedTopicStatus()
                }
                else -> {
                    logger.error("Setting export failed status",
                            "job_exit_status" to "${jobExecution.exitStatus}", "topic" to topicName)
                    exportStatusService.setFailedStatus()
                }
            }
        }
    }

    private fun sendSnapshotSenderMessages(jobExecution: JobExecution) {
        if (jobExecution.exitStatus.equals(ExitStatus.COMPLETED) && exportStatusService.exportedFilesCount() == 0) {
            messagingService.notifySnapshotSenderNoFilesExported()
        }
    }

    private fun sendAdgMessage(completionStatus: ExportCompletionStatus) {
        if (completionStatus.equals(ExportCompletionStatus.COMPLETED_SUCCESSFULLY) && triggerAdg.toBoolean()) {
            snsService.sendExportCompletedSuccessfullyMessage()
        }
    }


    private fun sendPdmCommonModelMessage(completionStatus: ExportCompletionStatus) {
        if (completionStatus.equals(ExportCompletionStatus.COMPLETED_SUCCESSFULLY) || completionStatus.equals(ExportCompletionStatus.COMPLETED_UNSUCCESSFULLY)) {
            messagingService.sendDataEgressMessage(pdmCommonModelSitePrefix)
        }
    }

    private fun sendDataEgressRisMessage(jobExecution: JobExecution) {
        if (jobExecution.exitStatus.equals(ExitStatus.COMPLETED) && sendToRis.toBoolean() && exportStatusService.exportedFilesCount() > 0) {
            messagingService.sendDataEgressMessage("$exportPrefix/$topicName-")
        }
    }

    private fun sendTopicFailedMonitoringMessage(jobExecution: JobExecution) {
        if (!jobExecution.exitStatus.equals(ExitStatus.COMPLETED)) {
            snsService.sendTopicFailedMonitoringMessage()
        }
    }

    private fun setProductStatus(completionStatus: ExportCompletionStatus) {
        when (completionStatus) {
            ExportCompletionStatus.COMPLETED_SUCCESSFULLY -> {
                productStatusService.setCompletedStatus()
            }
            ExportCompletionStatus.COMPLETED_UNSUCCESSFULLY -> {
                productStatusService.setFailedStatus()
            }
        }
    }

    private fun sendCompletionMonitoringMessage(completionStatus: ExportCompletionStatus) {
        if (completionStatus.equals(ExportCompletionStatus.COMPLETED_SUCCESSFULLY) || completionStatus.equals(ExportCompletionStatus.COMPLETED_UNSUCCESSFULLY)) {
            snsService.sendCompletionMonitoringMessage(completionStatus)
        }
    }

    private fun isATableUnavailableException(allFailureExceptions: MutableList<Throwable>) =
            allFailureExceptions.map(Throwable::cause).any { it is TableNotEnabledException || it is TableNotFoundException }

    private fun isABlockedTopicException(allFailureExceptions: MutableList<Throwable>) =
            allFailureExceptions.map(Throwable::cause).any { it is BlockedTopicException }

    @Value("\${topic.name}")
    private lateinit var topicName: String

    @Value("\${trigger.adg:false}")
    private lateinit var triggerAdg: String

    @Value("\${send.to.ris:false}")
    private lateinit var sendToRis: String

    @Value("\${snapshot.type}")
    private lateinit var snapshotType: String

    @Value("\${s3.prefix.folder}")
    private lateinit var exportPrefix: String

    @Value("\${pdm.common.model.site.prefix}")
    private lateinit var pdmCommonModelSitePrefix: String

    private val timer: Summary.Timer by lazy {
        topicDurationSummary.startTimer()
    }

    companion object {
        val logger = DataworksLogger.getLogger(S3StreamingWriter::class)
    }
}
