package app.services.impl

import app.services.MessagingService
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
@SpringBootTest(classes = [SQSMessagingService::class])
@TestPropertySource(properties = [
    "snapshot.sender.export.date=2020-06-05",
    "snapshot.sender.reprocess.files=false",
    "snapshot.sender.shutdown.flag=true",
    "snapshot.sender.sqs.queue.url=http://aws:4566/000000000000/snapshot-sender-queue",
    "snapshot.type=incremental",
    "sqs.retry.delay=1",
    "sqs.retry.maxAttempts=10",
    "sqs.retry.multiplier=1",
    "topic.name=db.database.collection",
    "data.egress.sqs.queue.url=http://aws:4566/000000000000/data-egress-queue",
    "trigger.snapshot.sender=true",
])
class SQSMessagingServiceTest {

    @Autowired
    private lateinit var messagingService: MessagingService

    @MockBean
    private lateinit var amazonSQS: AmazonSQS

    @Before
    fun init() {
        reset(amazonSQS)
        System.setProperty("correlation_id", "correlation-id")
        ReflectionTestUtils.setField(messagingService, "triggerSnapshotSender", "true")
    }

    @Test
    fun notifySnapshotSenderRetries() {
        val sendMessageResult = mock<SendMessageResult>()
        given(amazonSQS.sendMessage(any()))
            .willThrow(SdkClientException(""))
            .willThrow(SdkClientException(""))
            .willReturn(sendMessageResult)
        messagingService.notifySnapshotSender("db.collection")
        verify(amazonSQS, times(3)).sendMessage(any())
    }

    @Test
    fun notifySnapshotSenderNoFilesSentRetries() {
        val sendMessageResult = mock<SendMessageResult>()
        given(amazonSQS.sendMessage(any()))
            .willThrow(SdkClientException(""))
            .willThrow(SdkClientException(""))
            .willReturn(sendMessageResult)
        messagingService.notifySnapshotSenderNoFilesExported()
        verify(amazonSQS, times(3)).sendMessage(any())
        verifyNoMoreInteractions(amazonSQS)
    }


    @Test
    fun notifySnapshotSenderSendsCorrectMessageIfFlagTrue() {
        val sendMessageResult = mock<SendMessageResult>()
        given(amazonSQS.sendMessage(any())).willReturn(sendMessageResult)
        messagingService.notifySnapshotSender("db.collection")
        val expected = SendMessageRequest().apply {
            queueUrl = "http://aws:4566/000000000000/snapshot-sender-queue"
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
            messageGroupId = "db_database_collection"
        }
        verify(amazonSQS, times(1)).sendMessage(expected)
        verifyNoMoreInteractions(amazonSQS)
    }

    @Test
    fun triggerDataEgressSendsCorrectMessage() {
        val sendMessageResult = mock<SendMessageResult>()
        given(amazonSQS.sendMessage(any())).willReturn(sendMessageResult)
        messagingService.sendDataEgressMessage("db.collection")
        val expected = SendMessageRequest().apply {
            queueUrl = "http://aws:4566/000000000000/data-egress-queue"
            messageBody = """
            |{
            |    "Records": [
            |        {
            |            "s3": {
            |                "object": {
            |                    "key": "db.collection"
            |                }
            |            }
            |        }
            |    ]
            |}
            """.trimMargin()
        }
        verify(amazonSQS, times(1)).sendMessage(expected)
        verifyNoMoreInteractions(amazonSQS)
    }

    @Test
    fun triggerDataEgressSendsCorrectPDMMessage() {
        val sendMessageResult = mock<SendMessageResult>()
        given(amazonSQS.sendMessage(any())).willReturn(sendMessageResult)
        messagingService.sendDataEgressMessage("test/prefix/pipeline_success.flag")
        val expected = SendMessageRequest().apply {
            queueUrl = "http://aws:4566/000000000000/data-egress-queue"
            messageBody = """
            |{
            |    "Records": [
            |        {
            |            "s3": {
            |                "object": {
            |                    "key": "test/prefix/pipeline_success.flag"
            |                }
            |            }
            |        }
            |    ]
            |}
            """.trimMargin()
        }
        verify(amazonSQS, times(1)).sendMessage(expected)
        verifyNoMoreInteractions(amazonSQS)
    }

    @Test
    fun notifySnapshotSenderNoFilesExportedSendsCorrectMessageIfFlagTrue() {
        val sendMessageResult = mock<SendMessageResult>()
        given(amazonSQS.sendMessage(any())).willReturn(sendMessageResult)
        messagingService.notifySnapshotSenderNoFilesExported()
        val expected = SendMessageRequest().apply {
            queueUrl = "http://aws:4566/000000000000/snapshot-sender-queue"
            messageBody = """
            |{
            |   "shutdown_flag": "true",
            |   "correlation_id": "correlation-id",
            |   "topic_name": "db.database.collection",
            |   "export_date": "2020-06-05",
            |   "reprocess_files": "false",
            |   "snapshot_type": "incremental",
            |   "files_exported": 0
            |}
            """.trimMargin()
            messageGroupId = "db_database_collection"
        }

        verify(amazonSQS, times(1)).sendMessage(expected)
        verifyNoMoreInteractions(amazonSQS)
    }

    @Test
    fun notifySnapshotSenderDoesNotSendMessageIfFlagFalse() {
        ReflectionTestUtils.setField(messagingService, "triggerSnapshotSender", "false")
        messagingService.notifySnapshotSender("db.collection")
        verifyZeroInteractions(amazonSQS)
    }

    @Test
    fun notifySnapshotSenderNoFilesExportedDoesNotSendMessageIfFlagFalse() {
        ReflectionTestUtils.setField(messagingService, "triggerSnapshotSender", "false")
        messagingService.notifySnapshotSenderNoFilesExported()
        verifyZeroInteractions(amazonSQS)
    }
}
