package app.batch

import app.services.ExportStatusService
import app.utils.logging.logError
import app.utils.logging.logInfo
import org.apache.hadoop.hbase.TableNotEnabledException
import org.apache.hadoop.hbase.TableNotFoundException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.listener.JobExecutionListenerSupport
import org.springframework.stereotype.Component

@Component
class JobCompletionNotificationListener(private val exportStatusService: ExportStatusService) :
        JobExecutionListenerSupport() {

    override fun afterJob(jobExecution: JobExecution) {

        logInfo(logger, "Job completed", "exit_status", jobExecution.exitStatus.exitCode)

        if (jobExecution.exitStatus.equals(ExitStatus.COMPLETED)) {
            exportStatusService.setExportedStatus()
        } else {
            if (isATableUnavailableExceptions(jobExecution.allFailureExceptions)) {
                logError(logger,"Setting table unavailable status",
                        "job_exit_status", "${jobExecution.exitStatus}")
                exportStatusService.setTableUnavailableStatus()
            } else {
                logError(logger, "Setting export failed status",
                        "job_exit_status", "${jobExecution.exitStatus}")
                exportStatusService.setFailedStatus()
            }
        }
    }

    private fun isATableUnavailableExceptions(allFailureExceptions: MutableList<Throwable>) : Boolean {
        logInfo(logger, "Checking if table is unavailable exception", "failure_exceptions", allFailureExceptions.size.toString())
        allFailureExceptions.forEach {
            logInfo(logger, "Checking current failure exception", "failure_exception", it.localizedMessage, "cause", it.cause.toString(), "cause_message", it.message ?: "")
            if (it.cause is TableNotFoundException || it.cause is TableNotEnabledException) {
                return true
            }
        }
        return false
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(S3StreamingWriter::class.toString())
    }
}
