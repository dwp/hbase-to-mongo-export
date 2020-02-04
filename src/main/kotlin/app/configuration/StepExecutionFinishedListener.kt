package app.configuration

import app.batch.S3StreamingWriter
import app.batch.legacy.DirectoryWriter
import app.batch.legacy.FileSystemWriter
import app.batch.legacy.S3DirectoryWriter
import app.domain.Record
import app.utils.logging.logInfo
import org.slf4j.LoggerFactory
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.StepExecutionListener
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.listener.JobExecutionListenerSupport
import org.springframework.batch.item.ItemWriter
import org.springframework.stereotype.Component

//@Component
//@StepScope
class StepExecutionFinishedListener(private val writer: ItemWriter<Record>): StepExecutionListener {
    override fun beforeStep(stepExecution: StepExecution) {
    }

    override fun afterStep(stepExecution: StepExecution): ExitStatus? {
        if (writer is S3StreamingWriter) {
            writer.writeOutput()
        }
        return stepExecution.exitStatus
    }

    companion object {
        private val logger = LoggerFactory.getLogger(StepExecutionFinishedListener::class.java)
    }

}
