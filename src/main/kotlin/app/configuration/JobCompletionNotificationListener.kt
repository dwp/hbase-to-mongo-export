package app.configuration

import app.services.ExportStatusService
import app.utils.logging.logInfo
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.listener.JobExecutionListenerSupport
import org.springframework.stereotype.Component

@Component
class JobCompletionNotificationListener(private val exportStatusService: ExportStatusService) : JobExecutionListenerSupport() {

    override fun afterJob(jobExecution: JobExecution) {
        if (jobExecution.status == BatchStatus.COMPLETED) {
            logInfo(logger, "Setting status to exported")
            exportStatusService.setExportedStatus()
        }
        logInfo(logger, "Finished job", "job_status", "${jobExecution.status}")
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(JobCompletionNotificationListener::class.toString())
    }
}
