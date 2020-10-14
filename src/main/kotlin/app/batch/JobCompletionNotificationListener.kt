package app.batch

import app.services.ExportStatusService
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.listener.JobExecutionListenerSupport
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Component
class JobCompletionNotificationListener(private val exportStatusService: ExportStatusService):
        JobExecutionListenerSupport() {

    override fun afterJob(jobExecution: JobExecution) {
        logger.info("Job completed", "exit_status" to jobExecution.exitStatus.exitCode)
        if (jobExecution.exitStatus.equals(ExitStatus.COMPLETED)) {
            exportStatusService.setExportedStatus()
        }
        else {
            logger.error("Setting export failed status",
                    "job_exit_status" to "${jobExecution.exitStatus}")
            exportStatusService.setFailedStatus()
        }
    }

    companion object {
        val logger = DataworksLogger.getLogger(S3StreamingWriter::class.toString())
    }
}
