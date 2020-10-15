package app.services.impl

import app.services.SnapshotSenderMessagingService
import com.amazonaws.SdkClientException
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.amazonaws.services.sqs.model.SendMessageResult
import com.nhaarman.mockitokotlin2.*
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
@SpringBootTest(classes = [SnapshotSenderSQSMessagingService::class])
@TestPropertySource(properties = [
    "snapshot.sender.sqs.queue.url=http://aws:4566",
    "snapshot.sender.reprocess.files=false",
    "snapshot.sender.shutdown.flag=true",
    "snapshot.sender.export.date=2020-06-05",
    "topic.name=db.database.collection",
    "trigger.snapshot.sender=true",
    "snapshot.type=incremental",
    "sqs.retry.maxAttempts=10",
    "sqs.retry.delay=1",
    "sqs.retry.multiplier=1"
])
class SnapshotSenderSQSMessagingServiceTest {

    @Autowired
    private lateinit var snapshotSenderMessagingService: SnapshotSenderMessagingService

    @MockBean
    private lateinit var amazonSQS: AmazonSQS

    @Before
    fun init() {
        reset(amazonSQS)
        System.setProperty("correlation_id", "correlation-id")
        ReflectionTestUtils.setField(snapshotSenderMessagingService, "triggerSnapshotSender", "true")
    }

    @Test
    fun notifySnapshotSenderRetries() {
        val sendMessageResult = mock<SendMessageResult>()
        given(amazonSQS.sendMessage(any()))
                .willThrow(SdkClientException(""))
                .willThrow(SdkClientException(""))
                .willReturn(sendMessageResult)
        snapshotSenderMessagingService.notifySnapshotSender("db.collection")
        verify(amazonSQS, times(3)).sendMessage(any())
    }


    @Test
    fun notifySnapshotSenderSendsCorrectMessageIfFlagTrue() {
        val sendMessageResult = mock<SendMessageResult>()
        given(amazonSQS.sendMessage(any())).willReturn(sendMessageResult)
        snapshotSenderMessagingService.notifySnapshotSender("db.collection")
        val expected = SendMessageRequest().apply {
            queueUrl = "http://aws:4566"
            delaySeconds = 30
            messageBody = """
            |{
            |   "shutdown_flag": "true",
            |   "correlation_id": "correlation-id",
            |   "topic_name": "db.database.collection",
            |   "export_date": "2020-06-05",
            |   "reprocess_files": "false",
            |   "s3_full_folder": "db.collection",
            |   "snapshot_type": "incremental"
            |}
            """.trimMargin()
        }
        verify(amazonSQS, times(1)).sendMessage(expected)
        verifyNoMoreInteractions(amazonSQS)
    }


    @Test
    fun notifySnapshotSenderDoesNotSendMessageIfFlagFalse() {
        ReflectionTestUtils.setField(snapshotSenderMessagingService, "triggerSnapshotSender", "false")
        snapshotSenderMessagingService.notifySnapshotSender("db.collection")
        verifyZeroInteractions(amazonSQS)
    }
}
