package app.configuration

import app.services.ExportStatusService
import com.nhaarman.mockitokotlin2.*
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.JobExecution
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.retry.annotation.EnableRetry
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@EnableRetry
@SpringBootTest(classes = [JobCompletionNotificationListener::class])
class JobCompletionNotificationListenerTest {

    @Autowired
    private lateinit var jobCompletionNotificationListener: JobCompletionNotificationListener

    @MockBean
    private lateinit var exportStatusService: ExportStatusService

    @Test
    fun setsExportedStatusOnSuccessfulCompletion() {
        val batchStatus = BatchStatus.COMPLETED

        val jobExecution = mock<JobExecution> {
            on { status } doReturn batchStatus
        }

        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(exportStatusService, times(1)).setExportedStatus()
        verifyNoMoreInteractions(exportStatusService)
    }


    @Test
    fun doesNotSetExportedStatusOnUnsuccessfulCompletion() {
        val batchStatus = BatchStatus.UNKNOWN

        val jobExecution = mock<JobExecution> {
            on { status } doReturn batchStatus
        }

        jobCompletionNotificationListener.afterJob(jobExecution)
        verifyZeroInteractions(exportStatusService)
    }
}
