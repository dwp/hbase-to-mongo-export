package app.configuration

import app.batch.StreamingWriter
import app.batch.legacy.DirectoryWriter
import app.batch.legacy.FileSystemWriter
import app.batch.legacy.S3DirectoryWriter
import app.domain.Record
import app.utils.logging.logInfo
import org.slf4j.LoggerFactory
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.listener.JobExecutionListenerSupport
import org.springframework.batch.item.ItemWriter
import org.springframework.stereotype.Component

@Component
class JobCompletionNotificationListener(private val writer: ItemWriter<Record>) : JobExecutionListenerSupport() {

    override fun afterJob(jobExecution: JobExecution) {
        if (writer is StreamingWriter) {
            writer.writeOutput()
            logInfo(logger, "Finished through StreamingWriter, status : '${jobExecution.status}'.")
        } else if (writer is DirectoryWriter) {
            writer.writeOutput()
            logInfo(logger, "Finished through DirectoryWriter, status : '${jobExecution.status}'.")
        } else if (writer is S3DirectoryWriter) {
            writer.writeOutput()
            logInfo(logger, "Finished through S3DirectoryWriter, status : '${jobExecution.status}'.")
        } else  if (writer is FileSystemWriter) {
            writer.writeOutput()
            logInfo(logger, "Finished through FileSystemWriter, status : '${jobExecution.status}'.")
        }
        logInfo(logger, "Finished, status: '${jobExecution.status}'.")
    }

    companion object {
        private val logger = LoggerFactory.getLogger(JobCompletionNotificationListener::class.java)
    }
}
