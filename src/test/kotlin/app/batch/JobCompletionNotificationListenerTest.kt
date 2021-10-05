package app.batch

import app.exceptions.BlockedTopicException
import app.services.*
import app.utils.TextUtils
import com.nhaarman.mockitokotlin2.*
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Summary
import org.apache.hadoop.hbase.client.Connection
import org.junit.Before
import org.junit.Test
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.JobExecution
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor
import org.springframework.test.util.ReflectionTestUtils

class JobCompletionNotificationListenerTest {

    @Before
    fun before() {
        reset(messagingService)
        reset(snsService)
        reset(pushgatewayService)
        reset(postProcessor)
        reset(runningApplicationsGauge)
        reset(topicsStartedCounter)
        reset(topicsCompletedCounter)
        reset(productStatusService)
        reset(timer)
    }

    @Test
    fun willIncrementRunningApplicationsCountAndPushMetricsSuccessfully() {
        val exportStatusService = mock<ExportStatusService>()
        val jobCompletionNotificationListener = jobCompletionNotificationListener(exportStatusService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.EXECUTING
        }
        jobCompletionNotificationListener.beforeJob(jobExecution)
        verifyZeroInteractions(exportStatusService)
        verify(runningApplicationsGauge, times(1)).inc()
        verify(topicsStartedCounter, times(1)).inc()
        verifyNoMoreInteractions(topicsStartedCounter)
        verify(pushgatewayService, times(1)).pushMetrics()
        verifyNoMoreInteractions(pushgatewayService)
        verifyNoMoreInteractions(runningApplicationsGauge)
        verify(durationSummary, times(1)).startTimer()
        verifyNoMoreInteractions(durationSummary)
    }

    @Test
    fun setsExportedStatusOnSuccess() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.COMPLETED_SUCCESSFULLY
        }
        val jobCompletionNotificationListener = jobCompletionNotificationListener(exportStatusService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(exportStatusService, times(1)).setExportedStatus()
        verify(runningApplicationsGauge, times(1)).dec()
        verify(topicsCompletedCounter, times(1)).inc()
        verifyNoMoreInteractions(topicsCompletedCounter)
        verify(pushgatewayService, times(1)).pushFinalMetrics()
        verifyNoMoreInteractions(pushgatewayService)
        verifyNoMoreInteractions(runningApplicationsGauge)
        verify(timer, times(1)).observeDuration()
        verifyNoMoreInteractions(timer)
    }


    @Test
    fun setsFailedStatusOnFailure() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.COMPLETED_SUCCESSFULLY
        }

        val jobCompletionNotificationListener = jobCompletionNotificationListener(exportStatusService)
        ReflectionTestUtils.setField(jobCompletionNotificationListener, "topicName", TEST_TOPIC)
        ReflectionTestUtils.setField(jobCompletionNotificationListener, "pdmCommonModelSitePrefix", TEST_PDM_COMMON_MODEL_INPUTS_PREFIX)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.FAILED
            on { allFailureExceptions } doReturn listOf(Exception(Exception("Failed")))
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(snsService, times(1)).sendTopicFailedMonitoringMessage()
        verify(exportStatusService, times(1)).setFailedStatus()
        verify(runningApplicationsGauge, times(1)).dec()
        verify(topicsCompletedCounter, times(1)).inc()
        verifyNoMoreInteractions(topicsCompletedCounter)
        verify(pushgatewayService, times(1)).pushFinalMetrics()
        verifyNoMoreInteractions(pushgatewayService)
        verifyNoMoreInteractions(runningApplicationsGauge)
        verify(timer, times(1)).observeDuration()
        verifyNoMoreInteractions(timer)
    }

    @Test
    fun setsBlockedTopicStatusOnBlockedTopic() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.COMPLETED_SUCCESSFULLY
        }

        val jobCompletionNotificationListener = jobCompletionNotificationListener(exportStatusService)
        ReflectionTestUtils.setField(jobCompletionNotificationListener, "topicName", TEST_TOPIC)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.FAILED
            on { allFailureExceptions } doReturn listOf(Exception(BlockedTopicException("Blocked")))
        }

        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(snsService, times(1)).sendTopicFailedMonitoringMessage()
        verify(exportStatusService, times(1)).setBlockedTopicStatus()
        verify(runningApplicationsGauge, times(1)).dec()
        verify(pushgatewayService, times(1)).pushFinalMetrics()
        verifyNoMoreInteractions(pushgatewayService)
        verifyNoMoreInteractions(runningApplicationsGauge)
        verify(timer, times(1)).observeDuration()
        verifyNoMoreInteractions(timer)
    }

    @Test
    fun notifiesSnapshotSenderIfNoFilesExported() {
        ExportCompletionStatus.values().forEach { exportCompletionStatus ->
            val exportStatusService = mock<ExportStatusService> {
                on { exportedFilesCount() } doReturn 0
                on { exportCompletionStatus() } doReturn exportCompletionStatus
            }
            val jobCompletionNotificationListener = jobCompletionNotificationListener(exportStatusService)
            ReflectionTestUtils.setField(jobCompletionNotificationListener, "pdmCommonModelSitePrefix", TEST_PDM_COMMON_MODEL_INPUTS_PREFIX)
            val jobExecution = mock<JobExecution> {
                on { exitStatus } doReturn ExitStatus.COMPLETED
            }
            jobCompletionNotificationListener.afterJob(jobExecution)
            verify(messagingService, times(1)).notifySnapshotSenderNoFilesExported()
            verify(messagingService, times(1)).sendDataEgressMessage(TEST_PDM_COMMON_MODEL_INPUTS_PREFIX)
//            verifyNoMoreInteractions(messagingService)
            verify(pushgatewayService, times(1)).pushFinalMetrics()
            verifyNoMoreInteractions(pushgatewayService)
            reset(messagingService)
            reset(pushgatewayService)
            verify(timer, times(1)).observeDuration()
            verifyNoMoreInteractions(timer)
            reset(timer)
        }
    }

    @Test
    fun doesNotNotifySnapshotSenderIfFilesExported() {
        ExportCompletionStatus.values().forEach { exportCompletionStatus ->
            val exportStatusService = mock<ExportStatusService> {
                on { exportedFilesCount() } doReturn 1
                on { exportCompletionStatus() } doReturn exportCompletionStatus
            }
            val jobCompletionNotificationListener
                = jobCompletionNotificationListener(exportStatusService)
            val jobExecution = mock<JobExecution> {
                on { exitStatus } doReturn ExitStatus.COMPLETED
            }
            jobCompletionNotificationListener.afterJob(jobExecution)
            verify(messagingService, times(1)).sendDataEgressMessage("$S3_PREFIX/$TEST_TOPIC-")
            verify(messagingService, times(1)).sendDataEgressMessage(TEST_PDM_COMMON_MODEL_INPUTS_PREFIX)
//            verifyNoMoreInteractions(messagingService)
            verify(pushgatewayService, times(1)).pushFinalMetrics()
            verifyNoMoreInteractions(pushgatewayService)
            reset(messagingService)
            reset(pushgatewayService)
            verify(timer, times(1)).observeDuration()
            verifyNoMoreInteractions(timer)
            reset(timer)
        }
    }

    @Test
    fun doesNotNotifySnapshotSenderOnFailure() {
        ExportCompletionStatus.values().forEach { exportCompletionStatus ->
            val exportStatusService = mock<ExportStatusService> {
                on { exportedFilesCount() } doReturn 1
                on { exportCompletionStatus() } doReturn exportCompletionStatus
            }
            val jobCompletionNotificationListener = jobCompletionNotificationListener(exportStatusService)
//            ReflectionTestUtils.setField(jobCompletionNotificationListener, "pdmCommonModelSitePrefix", TEST_PDM_COMMON_MODEL_INPUTS_PREFIX)
            ReflectionTestUtils.setField(jobCompletionNotificationListener, "topicName", TEST_TOPIC)
            val jobExecution = mock<JobExecution> {
                on { exitStatus } doReturn ExitStatus.FAILED
            }
            jobCompletionNotificationListener.afterJob(jobExecution)
//            verifyZeroInteractions(messagingService)
//            verify(messagingService, times(1)).sendDataEgressMessage(TEST_PDM_COMMON_MODEL_INPUTS_PREFIX)
            verify(pushgatewayService, times(1)).pushFinalMetrics()
            verifyNoMoreInteractions(pushgatewayService)
            reset(messagingService)
            reset(pushgatewayService)
            verify(timer, times(1)).observeDuration()
            verifyNoMoreInteractions(timer)
            reset(timer)
        }
    }

    @Test
    fun setsProductCompletedStatusOnSuccess() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.COMPLETED_SUCCESSFULLY
        }
        val jobCompletionNotificationListener =
            jobCompletionNotificationListener(exportStatusService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(productStatusService, times(1)).setCompletedStatus()
        verifyNoMoreInteractions(productStatusService)
    }

    @Test
    fun setsProductFailedStatusOnFailure() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.COMPLETED_UNSUCCESSFULLY
        }
        val jobCompletionNotificationListener =
            jobCompletionNotificationListener(exportStatusService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(productStatusService, times(1)).setFailedStatus()
        verifyNoMoreInteractions(productStatusService)
    }

    @Test
    fun setsNoProductStatusWhenNotCompleted() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.NOT_COMPLETED
        }
        val jobCompletionNotificationListener =
            jobCompletionNotificationListener(exportStatusService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verifyZeroInteractions(productStatusService)
    }

    @Test
    fun setsNoProductStatusWhenInProgress() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.IN_PROGRESS
        }
        val jobCompletionNotificationListener =
            jobCompletionNotificationListener(exportStatusService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verifyZeroInteractions(productStatusService)
    }

    @Test
    fun sendsExportCompletedSuccessfullyMessageOnSuccess() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.COMPLETED_SUCCESSFULLY
        }
        val jobCompletionNotificationListener =
            jobCompletionNotificationListener(exportStatusService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(snsService, times(1)).sendExportCompletedSuccessfullyMessage()
        verify(snsService, times(1)).sendCompletionMonitoringMessage(any())
        verifyNoMoreInteractions(snsService)
        verify(runningApplicationsGauge, times(1)).dec()
        verify(pushgatewayService, times(1)).pushFinalMetrics()
        verifyNoMoreInteractions(runningApplicationsGauge)
        verifyNoMoreInteractions(pushgatewayService)
        verify(timer, times(1)).observeDuration()
        verifyNoMoreInteractions(timer)
    }

    @Test
    fun doesNotSendExportCompletedSuccessfullyMessageOnSuccessIfNotRequired() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.COMPLETED_SUCCESSFULLY
        }
        val jobCompletionNotificationListener =
            jobCompletionNotificationListener(exportStatusService, "false")
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(snsService, times(1)).sendCompletionMonitoringMessage(any())
        verifyNoMoreInteractions(snsService)
        verify(runningApplicationsGauge, times(1)).dec()
        verify(pushgatewayService, times(1)).pushFinalMetrics()
        verifyNoMoreInteractions(runningApplicationsGauge)
        verifyNoMoreInteractions(pushgatewayService)
        verify(timer, times(1)).observeDuration()
        verifyNoMoreInteractions(timer)
    }

    @Test
    fun doesNotSendExportCompletedSuccessfullyMessageOnFailure() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.COMPLETED_UNSUCCESSFULLY
        }
        val jobCompletionNotificationListener =
            jobCompletionNotificationListener(exportStatusService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(snsService, times(1)).sendCompletionMonitoringMessage(any())
        verifyNoMoreInteractions(snsService)
        verify(runningApplicationsGauge, times(1)).dec()
        verify(pushgatewayService, times(1)).pushFinalMetrics()
        verifyNoMoreInteractions(runningApplicationsGauge)
        verifyNoMoreInteractions(pushgatewayService)
        verify(timer, times(1)).observeDuration()
        verifyNoMoreInteractions(timer)
    }

    @Test
    fun doesNotSendAnyMessagesWhileNotCompleted() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.NOT_COMPLETED
        }
        val jobCompletionNotificationListener =
            jobCompletionNotificationListener(exportStatusService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(runningApplicationsGauge, times(1)).dec()
        verify(pushgatewayService, times(1)).pushFinalMetrics()
        verifyNoMoreInteractions(runningApplicationsGauge)
        verifyNoMoreInteractions(pushgatewayService)
        verify(timer, times(1)).observeDuration()
        verifyNoMoreInteractions(timer)
        verifyZeroInteractions(snsService)
    }

    @Test
    fun doesNotSendAnyMessagesWhileInProgress() {
        val exportStatusService = mock<ExportStatusService> {
            on { exportCompletionStatus() } doReturn ExportCompletionStatus.IN_PROGRESS
        }
        val jobCompletionNotificationListener =
            jobCompletionNotificationListener(exportStatusService)
        val jobExecution = mock<JobExecution> {
            on { exitStatus } doReturn ExitStatus.COMPLETED
        }
        jobCompletionNotificationListener.afterJob(jobExecution)
        verify(runningApplicationsGauge, times(1)).dec()
        verify(pushgatewayService, times(1)).pushFinalMetrics()
        verifyNoMoreInteractions(runningApplicationsGauge)
        verifyNoMoreInteractions(pushgatewayService)
        verify(timer, times(1)).observeDuration()
        verifyNoMoreInteractions(timer)
        verifyZeroInteractions(snsService)
    }

    private fun jobCompletionNotificationListener(exportStatusService: ExportStatusService,
                                                  triggerAdg: String = "true",
                                                  sendToRis: String = "true"): JobCompletionNotificationListener =
        JobCompletionNotificationListener(exportStatusService, productStatusService, messagingService,
            snsService, pushgatewayService, durationSummary, runningApplicationsGauge,
            topicsStartedCounter, topicsCompletedCounter, connection, textUtils).apply {
                    ReflectionTestUtils.setField(this, "triggerAdg", triggerAdg)
                    ReflectionTestUtils.setField(this, "sendToRis", sendToRis)
                    ReflectionTestUtils.setField(this, "snapshotType", "drift_testing_incremental")
                    ReflectionTestUtils.setField(this, "topicName", TEST_TOPIC)
                    ReflectionTestUtils.setField(this, "pdmCommonModelSitePrefix", TEST_PDM_COMMON_MODEL_INPUTS_PREFIX)
                    ReflectionTestUtils.setField(this, "exportPrefix", S3_PREFIX)
        }

    private val productStatusService = mock<ProductStatusService>()
    private val messagingService = mock<MessagingService>()
    private val snsService = mock<SnsService>()
    private val pushgatewayService = mock<PushGatewayService>()
    private val postProcessor = mock<ScheduledAnnotationBeanPostProcessor>()
    private val runningApplicationsGauge = mock<Gauge>()
    private val timer = mock<Summary.Timer>()
    private val durationSummary = mock<Summary> {
        on { startTimer() } doReturn timer
    }
    private val topicsStartedCounter = mock<Counter>()
    private val topicsCompletedCounter = mock<Counter>()
    private val connection = mock<Connection>()
    private val textUtils = TextUtils()
    companion object {
        private const val TEST_TOPIC = "db.test.topic"
        private const val TEST_PDM_COMMON_MODEL_INPUTS_PREFIX = "test/prefix/pipeline_success.flag"
        private const val S3_PREFIX = "data/2021-08-01/type"
    }
}
