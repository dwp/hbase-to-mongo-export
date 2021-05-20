package app.services.impl

import app.services.ExportCompletionStatus
import app.services.ProductStatusService
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
@SpringBootTest(classes = [DynamoDBProductStatusService::class])
@TestPropertySource(properties = [
    "dynamodb.retry.maxAttempts=10",
    "dynamodb.retry.delay=1",
    "dynamodb.retry.multiplier=1"
])
class DynamoDBProductStatusServiceTest {

    @SpyBean
    @Autowired
    private lateinit var productStatusService: ProductStatusService

    @MockBean
    private lateinit var amazonDynamoDB: AmazonDynamoDB

    @Before
    fun before()  {
        System.setProperty("correlation_id", "correlation.id")
        reset(amazonDynamoDB)
    }

    @Test
    fun setFailedSetsCorrectStatus() {
        given(amazonDynamoDB.updateItem(any()))
                .willReturn(mock())
        productStatusService.setFailedStatus()
        val argumentCaptor = argumentCaptor<UpdateItemRequest>()
        verify(amazonDynamoDB, times(1)).updateItem(argumentCaptor.capture())
        val actual = argumentCaptor.firstValue.expressionAttributeValues
        val expected =  mapOf(":x" to AttributeValue().apply { s = "FAILED" })
        assertEquals(actual, expected)
    }

    @Test
    fun setCompletedSetsCorrectStatus() {
        given(amazonDynamoDB.updateItem(any()))
                .willReturn(mock())
        productStatusService.setCompletedStatus()
        val argumentCaptor = argumentCaptor<UpdateItemRequest>()
        verify(amazonDynamoDB, times(1)).updateItem(argumentCaptor.capture())
        val actual = argumentCaptor.firstValue.expressionAttributeValues
        val expected =  mapOf(":x" to AttributeValue().apply { s = "COMPLETED" })
        assertEquals(actual, expected)
    }

    @Test
    fun setFailedStatusRetries() {
        given(amazonDynamoDB.updateItem(any()))
                .willThrow(SdkClientException(""))
                .willThrow(SdkClientException(""))
                .willReturn(mock())
        productStatusService.setFailedStatus()
        verify(amazonDynamoDB, times(3)).updateItem(any())
    }

    @Test
    fun setCompletedStatusRetries() {
        given(amazonDynamoDB.updateItem(any()))
                .willThrow(SdkClientException(""))
                .willThrow(SdkClientException(""))
                .willReturn(mock())
        productStatusService.setCompletedStatus()
        verify(amazonDynamoDB, times(3)).updateItem(any())
        verifyNoMoreInteractions(amazonDynamoDB)
    }
}
