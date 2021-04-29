package app.services.impl

import app.services.ExportCompletionStatus
import app.services.SnsService
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.model.PublishRequest
import com.nhaarman.mockitokotlin2.*
import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.retry.annotation.EnableRetry
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.test.util.ReflectionTestUtils

@RunWith(SpringRunner::class)
@EnableRetry
@SpringBootTest(classes = [SnsServiceImpl::class])
@TestPropertySource(properties = [
    "snapshot.sender.export.date=2020-12-12",
    "sns.retry.maxAttempts=10",
    "sns.retry.delay=1",
    "sns.retry.multiplier=1",
])
class SnsServiceImplTest {

    @Before
    fun before() {
        System.setProperty("correlation_id", "correlation.id")
        ReflectionTestUtils.setField(snsService, "fullTopicArn", TOPIC_ARN)
        ReflectionTestUtils.setField(snsService, "monitoringTopicArn", TOPIC_ARN)
        ReflectionTestUtils.setField(snsService, "snapshotType", "full")
        ReflectionTestUtils.setField(snsService, "s3prefix", "prefix")
        reset(amazonSNS)
    }

    @Test
    fun doesNotSendSuccessIfNoTopicConfigured() {
        ReflectionTestUtils.setField(snsService, "fullTopicArn", "")
        snsService.sendExportCompletedSuccessfullyMessage()
        verifyZeroInteractions(amazonSNS)
    }

    @Test
    fun retriesSuccessUntilSuccessful() {
        given(amazonSNS.publish(any()))
            .willThrow(RuntimeException("Error"))
            .willThrow(RuntimeException("Error")).willReturn(mock())
        snsService.sendExportCompletedSuccessfullyMessage()
        verify(amazonSNS, times(3)).publish(any())
        verifyNoMoreInteractions(amazonSNS)
    }


    @Test
    fun triggersAdg() {
        given(amazonSNS.publish(any())).willReturn(mock())
        snsService.sendExportCompletedSuccessfullyMessage()
        argumentCaptor<PublishRequest> {
            verify(amazonSNS, times(1)).publish(capture())
            assertEquals(TOPIC_ARN, firstValue.topicArn)
            assertEquals("""{
                "correlation_id": "correlation.id",
                "s3_prefix": "prefix",
                "snapshot_type": "full",
                "export_date": "2020-12-12"
            }""".trimMargin(), firstValue.message.trimMargin())
        }
        verifyNoMoreInteractions(amazonSNS)
    }


    @Test
    fun triggersAdgButNotPdmWhenSkipTriggerSet() {
        ReflectionTestUtils.setField(snsService, "skipPdmTrigger", "true")
        given(amazonSNS.publish(any())).willReturn(mock())
        snsService.sendExportCompletedSuccessfullyMessage()
        argumentCaptor<PublishRequest> {
            verify(amazonSNS, times(1)).publish(capture())
            assertEquals(TOPIC_ARN, firstValue.topicArn)
            assertEquals("""{
                "correlation_id": "correlation.id",
                "s3_prefix": "prefix",
                "snapshot_type": "full",
                "export_date": "2020-12-12",
                "skip_pdm_trigger": "true"
            }""".trimMargin(), firstValue.message.trimMargin())
        }
        verifyNoMoreInteractions(amazonSNS)
        ReflectionTestUtils.setField(snsService, "skipPdmTrigger", "")
    }


    @Test
    fun triggersAdgButDoesNotSendSkipPdmTriggerWhenIsIsNotSet() {
        ReflectionTestUtils.setField(snsService, "skipPdmTrigger", "NOT_SET")
        given(amazonSNS.publish(any())).willReturn(mock())
        snsService.sendExportCompletedSuccessfullyMessage()
        argumentCaptor<PublishRequest> {
            verify(amazonSNS, times(1)).publish(capture())
            assertEquals(TOPIC_ARN, firstValue.topicArn)
            assertEquals("""{
                "correlation_id": "correlation.id",
                "s3_prefix": "prefix",
                "snapshot_type": "full",
                "export_date": "2020-12-12"
            }""".trimMargin(), firstValue.message.trimMargin())
        }
        verifyNoMoreInteractions(amazonSNS)
        ReflectionTestUtils.setField(snsService, "skipPdmTrigger", "")
    }


    @Test
    fun givesUpSuccessAfterMaxTriesUntilSuccessful() {
        given(amazonSNS.publish(any())).willThrow(RuntimeException("Error"))
        try {
            snsService.sendExportCompletedSuccessfullyMessage()
            fail("Expected exception")
        } catch (e: Exception) {
            // expected
        }
        verify(amazonSNS, times(10)).publish(any())
        verifyNoMoreInteractions(amazonSNS)
    }

    @Test
    fun sendsTheCorrectMonitoringMessageOnSuccess() {
        given(amazonSNS.publish(any())).willReturn(mock())
        snsService.sendMonitoringMessage(ExportCompletionStatus.COMPLETED_SUCCESSFULLY)
        argumentCaptor<PublishRequest> {
            verify(amazonSNS, times(1)).publish(capture())
            assertEquals(TOPIC_ARN, firstValue.topicArn)
            assertEquals("""{
                "severity": "Critical",
                "notification_type": "Information",
                "slack_username": "HTME",
                "title_text": "Full - Export finished - success",
                "custom_elements": [
                    {
                        "key": "Export date",
                        "value": "2020-12-12"
                    },
                    {
                        "key": "Correlation Id",
                        "value": "correlation.id"
                    }
                ]
            }""", firstValue.message)
        }
        verifyNoMoreInteractions(amazonSNS)
    }

    @Test
    fun sendsTheCorrectMonitoringMessageOnFailure() {
        given(amazonSNS.publish(any())).willReturn(mock())
        snsService.sendMonitoringMessage(ExportCompletionStatus.COMPLETED_UNSUCCESSFULLY)
        argumentCaptor<PublishRequest> {
            verify(amazonSNS, times(1)).publish(capture())
            assertEquals(TOPIC_ARN, firstValue.topicArn)
            assertEquals("""{
                "severity": "High",
                "notification_type": "Warning",
                "slack_username": "HTME",
                "title_text": "Full - Export finished - failed",
                "custom_elements": [
                    {
                        "key": "Export date",
                        "value": "2020-12-12"
                    },
                    {
                        "key": "Correlation Id",
                        "value": "correlation.id"
                    }
                ]
            }""", firstValue.message)
        }
        verifyNoMoreInteractions(amazonSNS)
    }

    @Test
    fun doesNotSendMonitoringIfNoTopicConfigured() {
        ReflectionTestUtils.setField(snsService, "monitoringTopicArn", "")
        snsService.sendMonitoringMessage(ExportCompletionStatus.COMPLETED_SUCCESSFULLY)
        verifyZeroInteractions(amazonSNS)
    }

    @Test
    fun retriesMonitoringUntilSuccessful() {
        given(amazonSNS.publish(any()))
            .willThrow(RuntimeException("Error"))
            .willThrow(RuntimeException("Error")).willReturn(mock())
        snsService.sendMonitoringMessage(ExportCompletionStatus.COMPLETED_SUCCESSFULLY)
        verify(amazonSNS, times(3)).publish(any())
        verifyNoMoreInteractions(amazonSNS)
    }

    @Test
    fun givesUpMonitoringAfterMaxTriesUntilSuccessful() {
        given(amazonSNS.publish(any())).willThrow(RuntimeException("Error"))
        try {
            snsService.sendMonitoringMessage(ExportCompletionStatus.COMPLETED_SUCCESSFULLY                              )
            fail("Expected exception")
        } catch (e: Exception) {
            // expected
        }
        verify(amazonSNS, times(10)).publish(any())
        verifyNoMoreInteractions(amazonSNS)
    }

    @MockBean
    private lateinit var amazonSNS: AmazonSNS

    @Autowired
    private lateinit var snsService: SnsService

    companion object {
        private const val TOPIC_ARN = "arn:sns"
    }
}
