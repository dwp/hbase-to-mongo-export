package app.batch

import app.services.ExportStatusService
import app.utils.logging.logError
import app.utils.logging.logInfo
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.listener.JobExecutionListenerSupport
import org.springframework.stereotype.Component

@Component
class JobCompletionNotificationListener(private val exportStatusService: ExportStatusService):
        JobExecutionListenerSupport() {

    override fun afterJob(jobExecution: JobExecution) {
        logInfo(logger, "Job completed", "exit_status", jobExecution.exitStatus.exitCode)
        if (jobExecution.exitStatus.equals(ExitStatus.COMPLETED)) {
            exportStatusService.setExportedStatus()
        }
        else if (jobExecution.exitStatus.equals(ExitStatus.UNKNOWN)) {
            logError(logger,"Setting table unavailable status",
                    "job_exit_status", "${jobExecution.exitStatus}")
            exportStatusService.setTableUnavailableStatus()
        }
        else {
            logError(logger,"Setting export failed status",
                    "job_exit_status", "${jobExecution.exitStatus}")
            exportStatusService.setFailedStatus()
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(S3StreamingWriter::class.toString())
    }
}
