package app.services.impl

import app.services.ExportCompletionStatus
import app.services.ExportStatusService
import app.services.TableService
import com.amazonaws.SdkClientException
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemResult
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest
import com.nhaarman.mockitokotlin2.*
import io.prometheus.client.Counter
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.retry.annotation.EnableRetry
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@EnableRetry
@SpringBootTest(classes = [DynamoDBExportStatusService::class])
@TestPropertySource(properties = [
    "dynamodb.retry.maxAttempts=10",
    "dynamodb.retry.delay=1",
    "dynamodb.retry.multiplier=1"
])
class DynamoDBExportStatusServiceTest {

    @SpyBean
    @Autowired
    private lateinit var exportStatusService: ExportStatusService

    @MockBean
    private lateinit var amazonDynamoDB: AmazonDynamoDB

    @MockBean
    private lateinit var tableService: TableService

    @MockBean(name = "successfulCollectionCounter")
    private lateinit var successfulCollectionCounter: Counter

    @MockBean(name = "successfulNonEmptyCollectionCounter")
    private lateinit var successfulNonEmptyCollectionCounter: Counter

    @MockBean(name = "emptyCollectionCounter")
    private lateinit var emptyCollectionCounter: Counter

    @MockBean(name = "failedCollectionCounter")
    private lateinit var failedCollectionCounter: Counter

    @Before
    fun before()  {
        System.setProperty("correlation_id", "correlation.id")
        reset(amazonDynamoDB)
    }

    @Test
    fun textExportCompletionStatusSuccessful() {
        given(tableService.statuses()).willReturn(listOf("Exported", "Sent", "Received", "Success", "Table_Unavailable", "Blocked_Topic"))
        val actual = exportStatusService.exportCompletionStatus()
        assertEquals(ExportCompletionStatus.COMPLETED_SUCCESSFULLY, actual)
    }

    @Test
    fun textExportCompletionStatusUnsuccessful() {
        given(tableService.statuses()).willReturn(listOf("Exported", "Sent", "Received", "Success",
            "Table_Unavailable", "Blocked_Topic", "Export_Failed"))
        val actual = exportStatusService.exportCompletionStatus()
        assertEquals(ExportCompletionStatus.COMPLETED_UNSUCCESSFULLY, actual)
    }

    @Test
    fun textExportCompletionStatusInProgress() {
        given(tableService.statuses()).willReturn(listOf("Exported", "Sent", "Received", "Success",
            "Table_Unavailable", "Blocked_Topic", "Exporting"))
        val actual = exportStatusService.exportCompletionStatus()
        assertEquals(ExportCompletionStatus.NOT_COMPLETED, actual)
    }

    @Test
    fun incrementExportedCountRetries() {
        given(amazonDynamoDB.updateItem(any()))
                .willThrow(SdkClientException(""))
                .willThrow(SdkClientException(""))
                .willReturn(mock())
        exportStatusService.incrementExportedCount("")
        verify(amazonDynamoDB, times(3)).updateItem(any())
    }

    @Test
    fun setFailedSetsCorrectStatus() {
        given(amazonDynamoDB.updateItem(any()))
                .willReturn(mock())
        exportStatusService.setFailedStatus()
        val argumentCaptor = argumentCaptor<UpdateItemRequest>()
        verify(amazonDynamoDB, times(1)).updateItem(argumentCaptor.capture())
        val actual = argumentCaptor.firstValue.expressionAttributeValues
        val expected =  mapOf(":x" to AttributeValue().apply { s = "Export_Failed" })
        assertEquals(actual, expected)
        verify(failedCollectionCounter, times(1)).inc()
        verifyNoMoreInteractions(failedCollectionCounter)
        verifyZeroInteractions(successfulCollectionCounter)
        verifyZeroInteractions(successfulNonEmptyCollectionCounter)
        verifyZeroInteractions(emptyCollectionCounter)
    }

    @Test
    fun setExportedSetsCorrectStatus() {
        val result = mock<GetItemResult> {
            on { item } doReturn mapOf("FilesExported" to AttributeValue().apply { n = "10" })
        }
        given(amazonDynamoDB.getItem(any())).willReturn(result)
        given(amazonDynamoDB.updateItem(any()))
                .willReturn(mock())
        exportStatusService.setExportedStatus()
        val argumentCaptor = argumentCaptor<UpdateItemRequest>()
        verify(amazonDynamoDB, times(1)).updateItem(argumentCaptor.capture())
        val actual = argumentCaptor.firstValue.expressionAttributeValues
        val expected =  mapOf(":x" to AttributeValue().apply { s = "Exported" })
        assertEquals(actual, expected)
        verify(successfulCollectionCounter, times(1)).inc()
        verifyNoMoreInteractions(successfulCollectionCounter)
        verify(successfulNonEmptyCollectionCounter, times(1)).inc()
        verifyNoMoreInteractions(successfulNonEmptyCollectionCounter)
        verifyZeroInteractions(failedCollectionCounter)
        verifyZeroInteractions(emptyCollectionCounter)
    }

    @Test
    fun setExportedEmptyCollectionSetsCorrectStatus() {
        val result = mock<GetItemResult> {
            on { item } doReturn mapOf("FilesExported" to AttributeValue().apply { n = "0" })
        }
        given(amazonDynamoDB.getItem(any())).willReturn(result)
        given(amazonDynamoDB.updateItem(any()))
            .willReturn(mock())
        exportStatusService.setExportedStatus()
        val argumentCaptor = argumentCaptor<UpdateItemRequest>()
        verify(amazonDynamoDB, times(1)).updateItem(argumentCaptor.capture())
        val actual = argumentCaptor.firstValue.expressionAttributeValues
        val expected =  mapOf(":x" to AttributeValue().apply { s = "Exported" })
        assertEquals(actual, expected)
        verify(successfulCollectionCounter, times(1)).inc()
        verifyNoMoreInteractions(successfulCollectionCounter)
        verify(emptyCollectionCounter, times(1)).inc()
        verifyNoMoreInteractions(emptyCollectionCounter)
        verifyZeroInteractions(failedCollectionCounter)
        verifyZeroInteractions(successfulNonEmptyCollectionCounter)
    }

    @Test
    fun setExportedStatusRetries() {
        val result = mock<GetItemResult> {
            on { item } doReturn mapOf("FilesExported" to AttributeValue().apply { n = "10" })
        }
        given(amazonDynamoDB.getItem(any())).willReturn(result)
        given(amazonDynamoDB.updateItem(any()))
            .willReturn(mock())
        given(amazonDynamoDB.updateItem(any()))
                .willThrow(SdkClientException(""))
                .willThrow(SdkClientException(""))
                .willReturn(mock())
        exportStatusService.setExportedStatus()
        verify(amazonDynamoDB, times(3)).updateItem(any())
    }

    @Test
    fun setFailedStatusRetries() {
        given(amazonDynamoDB.updateItem(any()))
                .willThrow(SdkClientException(""))
                .willThrow(SdkClientException(""))
                .willReturn(mock())
        exportStatusService.setFailedStatus()
        verify(amazonDynamoDB, times(3)).updateItem(any())
    }

    @Test
    fun exportedFileCountRetries() {
        given(amazonDynamoDB.getItem(any()))
                .willThrow(SdkClientException(""))
                .willThrow(SdkClientException(""))
                .willReturn(mock())
        exportStatusService.exportedFilesCount()
        verify(amazonDynamoDB, times(3)).getItem(any())
        verifyNoMoreInteractions(amazonDynamoDB)
    }

    @Test
    fun exportedFileCountReturnsExportedFileCount() {
        val response = mock<GetItemResult> {
            on { item } doReturn mapOf("FilesExported" to AttributeValue().apply { n = "0" })
        }

        given(amazonDynamoDB.getItem(any()))
           .willReturn(response)
        val actual = exportStatusService.exportedFilesCount()
        assertEquals(0, actual)
        verify(amazonDynamoDB, times(1)).getItem(any())
        verifyNoMoreInteractions(amazonDynamoDB)
    }

    @Test
    fun exportedFileCountReturnsMinusOneIfAttributeMissing() {
        val response = mock<GetItemResult> {
            on { item } doReturn mapOf("OtherAttribute" to AttributeValue().apply { n = "0" })
        }
        given(amazonDynamoDB.getItem(any())).willReturn(response)
        val actual = exportStatusService.exportedFilesCount()
        assertEquals(-1, actual)
        verify(amazonDynamoDB, times(1)).getItem(any())
        verifyNoMoreInteractions(amazonDynamoDB)
    }

}
