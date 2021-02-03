package app.batch

import app.exceptions.BlockedTopicException
import app.services.*
import com.nhaarman.mockitokotlin2.*
import org.junit.Test
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.JobExecution
import org.springframework.test.util.ReflectionTestUtils

class JobCompletionNotificationListenerTest {

    @Test
    fun setsExportedStatusOnSuccess() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.COMPLETED_SUCCESSFULLY
        }
        val messagingService = mock<SnapshotSenderMessagingService>()
        val snsService = mock<SnsService>()
        val metricsService = mock<MetricsService>()
        val jobCompletionNotificationListener =
            JobCompletionNotificationListener(exportStatusService, messagingService, snsService, metricsService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(exportStatusService, times(1)).setExportedStatus()
    }


    @Test
    fun setsFailedStatusOnFailure() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.COMPLETED_SUCCESSFULLY
        }

        val messagingService = mock<SnapshotSenderMessagingService>()
        val snsService = mock<SnsService>()
        val metricsService = mock<MetricsService>()
        val jobCompletionNotificationListener =
            JobCompletionNotificationListener(exportStatusService, messagingService, snsService, metricsService)
        ReflectionTestUtils.setField(jobCompletionNotificationListener, "topicName", TEST_TOPIC)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.FAILED
            on { allFailureExceptions } doReturn listOf(Exception(Exception("Failed")))
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(exportStatusService, times(1)).setFailedStatus()
    }

    @Test
    fun setsBlockedTopicStatusOnBlockedTopic() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.COMPLETED_SUCCESSFULLY
        }

        val messagingService = mock<SnapshotSenderMessagingService>()
        val metricsService = mock<MetricsService>()
        val snsService = mock<SnsService>()
        val jobCompletionNotificationListener =
            JobCompletionNotificationListener(exportStatusService, messagingService, snsService, metricsService)
        ReflectionTestUtils.setField(jobCompletionNotificationListener, "topicName", TEST_TOPIC)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.FAILED
            on { allFailureExceptions } doReturn listOf(Exception(BlockedTopicException("Blocked")))
        }

        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(exportStatusService, times(1)).setBlockedTopicStatus()
    }

    @Test
    fun notifiesSnapshotSenderIfNoFilesExported() {
        ExportCompletionStatus.values().forEach { exportCompletionStatus ->
            val exportStatusService = mock<ExportStatusService> {
                on { exportedFilesCount() } doReturn 0
                on { exportCompletionStatus() } doReturn exportCompletionStatus
            }
            val messagingService = mock<SnapshotSenderMessagingService>()
            val snsService = mock<SnsService>()
            val metricsService = mock<MetricsService>()
            val jobCompletionNotificationListener
                = JobCompletionNotificationListener(exportStatusService, messagingService, snsService, metricsService)
            val jobExecution = mock<JobExecution> {
                on { exitStatus } doReturn ExitStatus.COMPLETED
            }
            jobCompletionNotificationListener.afterJob(jobExecution)
            verify(messagingService, times(1)).notifySnapshotSenderNoFilesExported()
            verifyNoMoreInteractions(messagingService)
        }
    }

    @Test
    fun doesNotNotifySnapshotSenderIfFilesExported() {
        ExportCompletionStatus.values().forEach { exportCompletionStatus ->
            val exportStatusService = mock<ExportStatusService> {
                on { exportedFilesCount() } doReturn 1
                on { exportCompletionStatus() } doReturn exportCompletionStatus
            }
            val messagingService = mock<SnapshotSenderMessagingService>()
            val snsService = mock<SnsService>()
            val metricsService = mock<MetricsService>()
            val jobCompletionNotificationListener
                = JobCompletionNotificationListener(exportStatusService, messagingService, snsService, metricsService)
            val jobExecution = mock<JobExecution> {
                on { exitStatus } doReturn ExitStatus.COMPLETED
            }
            jobCompletionNotificationListener.afterJob(jobExecution)
            verifyZeroInteractions(messagingService)
        }
    }

    @Test
    fun doesNotNotifySnapshotSenderOnFailure() {
        ExportCompletionStatus.values().forEach { exportCompletionStatus ->
            val exportStatusService = mock<ExportStatusService> {
                on { exportedFilesCount() } doReturn 1
                on { exportCompletionStatus() } doReturn exportCompletionStatus
            }
            val messagingService = mock<SnapshotSenderMessagingService>()
            val metricsService = mock<MetricsService>()
            val snsService = mock<SnsService>()
            val jobCompletionNotificationListener = JobCompletionNotificationListener(exportStatusService, messagingService, snsService, metricsService)
            ReflectionTestUtils.setField(jobCompletionNotificationListener, "topicName", TEST_TOPIC)
            val jobExecution = mock<JobExecution> {
                on { exitStatus } doReturn ExitStatus.FAILED
            }
            jobCompletionNotificationListener.afterJob(jobExecution)
            verifyZeroInteractions(messagingService)
        }
    }

    @Test
    fun sendsExportCompletedSuccessfullyMessageOnSuccess() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.COMPLETED_SUCCESSFULLY
        }
        val messagingService = mock<SnapshotSenderMessagingService>()
        val snsService = mock<SnsService>()
        val metricsService = mock<MetricsService>()
        val jobCompletionNotificationListener =
            JobCompletionNotificationListener(exportStatusService, messagingService, snsService, metricsService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(snsService, times(1)).sendExportCompletedSuccessfullyMessage()
        verify(snsService, times(1)).sendMonitoringMessage(any())
        verifyNoMoreInteractions(snsService)
    }

    @Test
    fun doesNotSendExportCompletedSuccessfullyMessageOnFailure() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.COMPLETED_UNSUCCESSFULLY
        }
        val messagingService = mock<SnapshotSenderMessagingService>()
        val metricsService = mock<MetricsService>()
        val snsService = mock<SnsService>()
        val jobCompletionNotificationListener =
            JobCompletionNotificationListener(exportStatusService, messagingService, snsService, metricsService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(snsService, times(1)).sendMonitoringMessage(any())
        verifyNoMoreInteractions(snsService)
    }

    @Test
    fun doesNotSendAnyMessagesWhileInProgress() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.NOT_COMPLETED
        }
        val messagingService = mock<SnapshotSenderMessagingService>()
        val metricsService = mock<MetricsService>()
        val snsService = mock<SnsService>()
        val jobCompletionNotificationListener =
            JobCompletionNotificationListener(exportStatusService, messagingService, snsService, metricsService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verifyZeroInteractions(snsService)
    }

    companion object {
        private const val TEST_TOPIC = "db.test.topic"
    }
}
