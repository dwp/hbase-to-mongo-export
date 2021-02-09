@file:Suppress("ReplaceCallWithBinaryOperator")

package app.batch

import app.exceptions.BlockedTopicException
import app.services.*
import org.apache.hadoop.hbase.TableNotEnabledException
import org.apache.hadoop.hbase.TableNotFoundException
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.listener.JobExecutionListenerSupport
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger




@Component
class JobCompletionNotificationListener(private val exportStatusService: ExportStatusService,
                                        private val messagingService: SnapshotSenderMessagingService,
                                        private val snsService: SnsService,
                                        private val pushGatewayService: PushGatewayService): JobExecutionListenerSupport() {

    override fun afterJob(jobExecution: JobExecution) {
        try {
            logger.info("Job completed", "exit_status" to jobExecution.exitStatus.exitCode)
            setExportStatus(jobExecution)
            sendSqsMessages(jobExecution)
            sendSnsMessages()
        } finally {
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

    private fun sendSqsMessages(jobExecution: JobExecution) {
        if (jobExecution.exitStatus.equals(ExitStatus.COMPLETED) && exportStatusService.exportedFilesCount() == 0) {
            messagingService.notifySnapshotSenderNoFilesExported()
        }
    }

    private fun sendSnsMessages() {
        when (val completionStatus = exportStatusService.exportCompletionStatus()) {
            ExportCompletionStatus.COMPLETED_SUCCESSFULLY -> {
                if (triggerAdg.toBoolean()) {
                    snsService.sendExportCompletedSuccessfullyMessage()
                }
                snsService.sendMonitoringMessage(completionStatus)
            }
            ExportCompletionStatus.COMPLETED_UNSUCCESSFULLY -> {
                snsService.sendMonitoringMessage(completionStatus)
            }
        }
    }

    private fun isATableUnavailableException(allFailureExceptions: MutableList<Throwable>)  =
            allFailureExceptions.map(Throwable::cause).any { it is TableNotEnabledException || it is TableNotFoundException }

    private fun isABlockedTopicException(allFailureExceptions: MutableList<Throwable>)  =
            allFailureExceptions.map(Throwable::cause).any { it is BlockedTopicException }

    @Value("\${topic.name}")
    private lateinit var topicName: String

    @Value("\${trigger.adg:false}")
    private lateinit var triggerAdg: String

    companion object {
        val logger = DataworksLogger.getLogger(S3StreamingWriter::class)
    }
}
