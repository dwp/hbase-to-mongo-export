package app.services.impl

import app.configuration.HttpClientProvider
import app.domain.DataKeyResult
import app.exceptions.DataKeyDecryptionException
import app.exceptions.DataKeyServiceUnavailableException
import app.utils.UUIDGenerator
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.sqs.AmazonSQS
import com.google.gson.Gson
import com.nhaarman.mockitokotlin2.*
import org.apache.http.HttpEntity
import org.apache.http.StatusLine
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.CloseableHttpClient
import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.test.util.ReflectionTestUtils
import java.io.ByteArrayInputStream

@RunWith(SpringRunner::class)
@ActiveProfiles("aesCipherService", "httpDataKeyService", "unitTest", "outputToConsole")
@SpringBootTest
@TestPropertySource(properties = [
    "data.key.service.url=https://dks:8443",
    "hbase.zookeeper.quorum=hbase",
    "keyservice.retry.delay=1",
    "keyservice.retry.maxAttempts=5",
    "keyservice.retry.multiplier=1",
    "pushgateway.address=pushgateway:9090",
    "s3.bucket=bucket",
    "s3.prefix.folder=prefix",
    "snapshot.sender.export.date=2020-06-05",
    "snapshot.sender.reprocess.files=true",
    "snapshot.sender.shutdown.flag=true",
    "snapshot.sender.sqs.queue.url=http://aws:4566",
    "snapshot.type=full",
    "topic.name=db.a.b",
    "trigger.snapshot.sender=false",
])
class HttpKeyServiceTest {

    @Autowired
    private lateinit var keyService: HttpKeyService

    @Autowired
    private lateinit var httpClientProvider: HttpClientProvider

    @Autowired
    private lateinit var uuidGenerator: UUIDGenerator

    companion object {
        private val datakeyServiceResponseBody = """
                |{
                |    "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
                |    "plaintextDataKey": "PLAINTEXT_DATAKEY",
                |    "ciphertextDataKey": "CIPHERTEXT_DATAKEY"
                |}
            """.trimMargin()

        private var dksCorrelationId = 0

        private fun nextDksCorrelationId(): String {
            return "dks-id-${++dksCorrelationId}"
        }
    }

    @Before
    fun init() {
        this.keyService.clearCache()
        reset(this.httpClientProvider)
        reset(this.uuidGenerator)
        ReflectionTestUtils.setField(keyService, "dataKeyResult", null)
    }

    @Test
    fun shouldCacheDatakey() {
        val responseBody = datakeyServiceResponseBody

        val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())

        val status = mock<StatusLine> {
            on { statusCode } doReturn 201
        }

        val httpEntity = mock<HttpEntity> {
            on { content } doReturn byteArrayInputStream
        }

        val httpResponse = mock<CloseableHttpResponse> {
            on { statusLine } doReturn status
            on { entity } doReturn httpEntity
        }

        val httpClient = mock<CloseableHttpClient> {
            on { execute(any<HttpGet>()) } doReturn httpResponse
        }
        given(httpClient.execute(any<HttpGet>())).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        for (i in 0..99) {
            val result = keyService.batchDataKey()
            val expectedResult: DataKeyResult = Gson().fromJson(responseBody, DataKeyResult::class.java)
            assertEquals(expectedResult, result)
        }
        verify(httpClientProvider, times(1)).client()
        verifyNoMoreInteractions(httpClientProvider)
        verify(httpClient, times(1)).execute(any())
        verify(httpClient, times(1)).close()
        verifyNoMoreInteractions(httpClient)
    }

    @Test
    fun testBatchDataKey_WillCallClientOnce_AndReturnKey() {
        val responseBody = datakeyServiceResponseBody

        val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
        val statusLine = mock<StatusLine>()
        val entity = mock<HttpEntity>()
        given(entity.content).willReturn(byteArrayInputStream)
        given(statusLine.statusCode).willReturn(201)
        val httpResponse = mock<CloseableHttpResponse>()
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock<CloseableHttpClient>()
        given(httpClient.execute(any<HttpGet>())).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.batchDataKey()

        val expectedResult: DataKeyResult = Gson().fromJson(responseBody, DataKeyResult::class.java)
        assertEquals(expectedResult, dataKeyResult)

        val argumentCaptor = argumentCaptor<HttpGet>()
        verify(httpClient, times(1)).execute(argumentCaptor.capture())
        assertEquals("https://dks:8443/datakey?correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
    }

    @Test
    fun testBatchDataKey_ServerError_ThrowsException_AndWillRetry() {
        val httpClient = mock<CloseableHttpClient>()
        val statusLine = mock<StatusLine>()
        //val entity = mock(HttpEntity::class.java)
        given(statusLine.statusCode).willReturn(503)
        val httpResponse = mock<CloseableHttpResponse>()
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpClient.execute(any<HttpGet>())).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.batchDataKey()
            fail("Should throw a DataKeyServiceUnavailableException")
        } catch (ex: DataKeyServiceUnavailableException) {
            assertEquals("Getting batch data key - data key service returned bad status code '503' for dks_correlation_id: '$dksCallId'", ex.message)
            val argumentCaptor = argumentCaptor<HttpGet>()
            verify(httpClient, times(5)).execute(argumentCaptor.capture())
            assertEquals("https://dks:8443/datakey?correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @Test
    @Throws(DataKeyServiceUnavailableException::class)
    fun testBatchDataKey_UnknownHttpError_ThrowsException_AndWillRetry() {
        val statusLine = mock<StatusLine>()
        //val entity = mock(HttpEntity::class.java)
        given(statusLine.statusCode).willReturn(503)
        val httpResponse = mock<CloseableHttpResponse>()
        given(httpResponse.statusLine).willReturn(statusLine)
        val httpClient = mock<CloseableHttpClient>()
        given(httpClient.execute(any<HttpGet>())).willThrow(RuntimeException("Boom!"))
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.batchDataKey()
            fail("Should throw a DataKeyServiceUnavailableException")
        } catch (ex: DataKeyServiceUnavailableException) {
            assertEquals("Error contacting data key service: 'java.lang.RuntimeException: Boom!' for dks_correlation_id: '$dksCallId'", ex.message)
            val argumentCaptor = argumentCaptor<HttpGet>()
            verify(httpClient, times(5)).execute(argumentCaptor.capture())
            assertEquals("https://dks:8443/datakey?correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @Test
    @Throws(DataKeyServiceUnavailableException::class)
    fun testBatchDataKey_WhenErrorsOccur_WillRetryUntilSuccessful() {
        val responseBody = datakeyServiceResponseBody

        val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
        val statusLine = mock<StatusLine>()
        val entity = mock<HttpEntity>()
        given(entity.content).willReturn(byteArrayInputStream)
        given(statusLine.statusCode).willReturn(503, 503, 201)
        val httpResponse = mock<CloseableHttpResponse>()
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock<CloseableHttpClient>()
        given(httpClient.execute(any<HttpGet>())).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.batchDataKey()

        val expectedResult: DataKeyResult = Gson().fromJson(responseBody, DataKeyResult::class.java)
        assertEquals(expectedResult, dataKeyResult)

        val argumentCaptor = argumentCaptor<HttpGet>()
        verify(httpClient, times(3)).execute(argumentCaptor.capture())
        assertEquals("https://dks:8443/datakey?correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
    }

    @Test
    fun testDecryptKey_HappyCase_CallsServerOnce_AndReturnsUnencryptedData() {
        val responseBody = """
            |{
            |  "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
            |  "plaintextDataKey": "PLAINTEXT_DATAKEY"
            |}
        """.trimMargin()

        val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
        val statusLine = mock<StatusLine>()
        val entity = mock<HttpEntity>()
        given(entity.content).willReturn(byteArrayInputStream)
        given(statusLine.statusCode).willReturn(200)
        val httpResponse = mock<CloseableHttpResponse>()
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock<CloseableHttpClient>()
        given(httpClient.execute(any<HttpPost>())).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")

        assertEquals("PLAINTEXT_DATAKEY", dataKeyResult)
        val argumentCaptor = argumentCaptor<HttpPost>()
        verify(httpClient, times(1)).execute(argumentCaptor.capture())
        assertEquals("https://dks:8443/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
    }

    @Test
    fun testDecryptKey_WhenErrorOccur_WillRetryUntilSuccessful() {
        val responseBody = """
            |{
            |  "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
            |  "plaintextDataKey": "PLAINTEXT_DATAKEY"
            |}
        """.trimMargin()

        val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
        val mockStatusLine = mock<StatusLine>()
        val entity = mock<HttpEntity>()
        given(entity.content).willReturn(byteArrayInputStream)
        given(mockStatusLine.statusCode).willReturn(503, 503, 200)
        val httpResponse = mock<CloseableHttpResponse>()
        given(httpResponse.statusLine).willReturn(mockStatusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock<CloseableHttpClient>()
        given(httpClient.execute(any<HttpPost>())).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")

        assertEquals("PLAINTEXT_DATAKEY", dataKeyResult)

        val argumentCaptor = argumentCaptor<HttpPost>()
        verify(httpClient, times(3)).execute(argumentCaptor.capture())
        assertEquals("https://dks:8443/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
    }

    @Test
    fun testDecryptKey_HappyCase_WillCallServerOnce_AndCacheResponse() {
        val responseBody = """
            |{
            |  "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
            |  "plaintextDataKey": "PLAINTEXT_DATAKEY"
            |}
        """.trimMargin()

        val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
        val statusLine = mock<StatusLine>()
        val entity = mock<HttpEntity>()
        given(entity.content).willReturn(byteArrayInputStream)
        given(statusLine.statusCode).willReturn(200)
        val httpResponse = mock<CloseableHttpResponse>()
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock<CloseableHttpClient>()
        given(httpClient.execute(any<HttpPost>())).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
        assertEquals("PLAINTEXT_DATAKEY", dataKeyResult)

        keyService.decryptKey("123", "ENCRYPTED_KEY_ID")

        val argumentCaptor = argumentCaptor<HttpPost>()
        verify(httpClient, times(1)).execute(argumentCaptor.capture())
        assertEquals("https://dks:8443/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
    }

    @Test
    fun testDecryptKey_WithABadKey_WillCallServerOnce_AndNotRetry() {
        val statusLine = mock<StatusLine>()
        given(statusLine.statusCode).willReturn(400)
        val httpResponse = mock<CloseableHttpResponse>()
        given(httpResponse.statusLine).willReturn(statusLine)
        val httpClient = mock<CloseableHttpClient>()
        given(httpClient.execute(any())).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
            fail("Should throw a DataKeyDecryptionException")
        } catch (ex: DataKeyDecryptionException) {
            assertEquals("Decrypting encryptedKey: 'ENCRYPTED_KEY_ID' with keyEncryptionKeyId: '123' data key service returned status code '400' for dks_correlation_id: '$dksCallId'", ex.message)
            val argumentCaptor = argumentCaptor<HttpPost>()
            verify(httpClient, times(1)).execute(argumentCaptor.capture())
            assertEquals("https://dks:8443/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @Test
    fun testDecryptKey_ServerError_WillCauseRetryMaxTimes() {
        val statusLine = mock<StatusLine>()
        given(statusLine.statusCode).willReturn(503)
        val httpResponse = mock<CloseableHttpResponse>()
        given(httpResponse.statusLine).willReturn(statusLine)
        val httpClient = mock<CloseableHttpClient>()
        given(httpClient.execute(any<HttpPost>())).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
            fail("Should throw a DataKeyServiceUnavailableException")
        } catch (ex: DataKeyServiceUnavailableException) {
            assertEquals("Decrypting encryptedKey: 'ENCRYPTED_KEY_ID' with keyEncryptionKeyId: '123' data key service returned status code '503' for dks_correlation_id: '$dksCallId'", ex.message)
            val argumentCaptor = argumentCaptor<HttpPost>()
            verify(httpClient, times(5)).execute(argumentCaptor.capture())
            assertEquals("https://dks:8443/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @Test
    fun testDecryptKey_UnknownHttpError_WillCauseRetryMaxTimes() {
        val statusLine = mock<StatusLine>()
        given(statusLine.statusCode).willReturn(503)
        val httpResponse = mock<CloseableHttpResponse>()
        given(httpResponse.statusLine).willReturn(statusLine)
        val httpClient = mock<CloseableHttpClient>()
        given(httpClient.execute(any<HttpPost>())).willThrow(RuntimeException("Boom!"))
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
            fail("Should throw a DataKeyServiceUnavailableException")
        } catch (ex: DataKeyServiceUnavailableException) {
            assertEquals("Error contacting data key service: 'java.lang.RuntimeException: Boom!' for dks_correlation_id: '$dksCallId'", ex.message)
            val argumentCaptor = argumentCaptor<HttpPost>()
            verify(httpClient, times(5)).execute(argumentCaptor.capture())
            assertEquals("https://dks:8443/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @MockBean
    private lateinit var amazonDynamoDb: AmazonDynamoDB

    @MockBean
    private lateinit var amazonSQS: AmazonSQS

}
