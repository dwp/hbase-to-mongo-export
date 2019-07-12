package app.configuration

import app.batch.DirectoryWriter
import org.slf4j.LoggerFactory
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.listener.JobExecutionListenerSupport
import org.springframework.batch.item.ItemWriter
import org.springframework.stereotype.Component


@Component
class JobCompletionNotificationListener(private val writer: ItemWriter<String>): JobExecutionListenerSupport() {

    override fun afterJob(jobExecution: JobExecution) {
        logger.info("Finished, status: '${jobExecution.status}', writer: $writer.")

        if (writer is DirectoryWriter) {
            writer.closeOutput()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(JobCompletionNotificationListener::class.java)
    }
}