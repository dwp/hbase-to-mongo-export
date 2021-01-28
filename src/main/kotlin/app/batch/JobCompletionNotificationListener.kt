package app.batch

import app.exceptions.BlockedTopicException
import app.services.ExportStatusService
import app.services.SnapshotSenderMessagingService
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
                                        private val messagingService: SnapshotSenderMessagingService) :
        JobExecutionListenerSupport() {

    override fun afterJob(jobExecution: JobExecution) {
        logger.info("Job completed", "exit_status" to jobExecution.exitStatus.exitCode)
        if (jobExecution.exitStatus.equals(ExitStatus.COMPLETED)) {
            exportStatusService.setExportedStatus()
            if (exportStatusService.exportedFilesCount() == 0) {
                messagingService.notifySnapshotSenderNoFilesExported()
            }
        }
        else {
            when {
                isATableUnavailableExceptions(jobExecution.allFailureExceptions) -> {
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

    private fun isATableUnavailableExceptions(allFailureExceptions: MutableList<Throwable>) : Boolean {
        logger.info("Checking if table is unavailable exception",
                "failure_exceptions" to allFailureExceptions.size.toString())
        allFailureExceptions.forEach {
            logger.info("Checking current failure exception",
                    "failure_exception" to it.localizedMessage,
                    "cause" to it.cause.toString(),
                    "cause_message" to (it.message ?: ""))
            if (it.cause is TableNotFoundException || it.cause is TableNotEnabledException) {
                return true
            }
        }
        return false
    }

    private fun isABlockedTopicException(allFailureExceptions: MutableList<Throwable>) : Boolean {
        logger.info("Checking if blocked topic exception",
                "failure_exceptions" to allFailureExceptions.size.toString())
        allFailureExceptions.forEach {
            logger.info("Checking current failure exception",
                    "failure_exception" to it.localizedMessage,
                    "cause" to it.cause.toString(),
                    "cause_message" to (it.message ?: ""))
            if (it.cause is BlockedTopicException) {
                return true
            }
        }
        return false
    }

    @Value("\${topic.name}")
    private lateinit var topicName: String // i.e. "db.user.data"

    companion object {
        val logger = DataworksLogger.getLogger(S3StreamingWriter::class)
    }
}
