package app.services.impl

import app.configuration.HttpClientProvider
import app.domain.DataKeyResult
import app.exceptions.DataKeyDecryptionException
import app.exceptions.DataKeyServiceUnavailableException
import app.utils.UUIDGenerator
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.sqs.AmazonSQS
import com.google.gson.Gson
import com.nhaarman.mockitokotlin2.firstValue
import com.nhaarman.mockitokotlin2.whenever
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
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.BDDMockito.given
import org.mockito.Mockito.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.ByteArrayInputStream

@RunWith(SpringRunner::class)
@ActiveProfiles("aesCipherService", "httpDataKeyService", "unitTest", "outputToConsole")
@SpringBootTest
@TestPropertySource(properties = [
    "data.table.name=ucfs-data",
    "data.key.service.url=dummy.com:8090",
    "column.family=topic",
    "topic.name=db.a.b",
    "identity.keystore=resources/identity.jks",
    "trust.keystore=resources/truststore.jks",
    "identity.store.password=changeit",
    "identity.key.password=changeit",
    "trust.store.password=changeit",
    "identity.store.alias=cid",
    "hbase.zookeeper.quorum=hbase",
    "s3.bucket=bucket",
    "s3.prefix.folder=prefix",
    "snapshot.sender.sqs.queue.url=http://aws:4566",
    "snapshot.sender.sqs.queue.url=http://aws:4566",
    "snapshot.sender.reprocess.files=true",
    "snapshot.sender.shutdown.flag=true",
    "snapshot.sender.export.date=2020-06-05",
    "trigger.snapshot.sender=false",
    "snapshot.type=full",
    "keyservice.retry.maxAttempts=5",
    "keyservice.retry.delay=1",
    "keyservice.retry.multiplier=1"
])
class HttpKeyServiceTest {

    @Autowired
    private lateinit var keyService: HttpKeyService

    @Autowired
    private lateinit var httpClientProvider: HttpClientProvider

    @Autowired
    private lateinit var uuidGenerator: UUIDGenerator

    companion object {
        var dksCorrelationId = 0

        private fun nextDksCorrelationId(): String {
            return "dks-id-${++dksCorrelationId}"
        }
    }

    @Before
    fun init() {
        this.keyService.clearCache()
        reset(this.httpClientProvider)
        reset(this.uuidGenerator)
    }


    @Test
    fun testBatchDataKey_WillCallClientOnce_AndReturnKey() {
        val responseBody = """
            |{
            |    "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
            |    "plaintextDataKey": "PLAINTEXT_DATAKEY",
            |    "ciphertextDataKey": "CIPHERTEXT_DATAKEY"
            |}
        """.trimMargin()

        val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
        val statusLine = mock(StatusLine::class.java)
        val entity = mock(HttpEntity::class.java)
        given(entity.content).willReturn(byteArrayInputStream)
        given(statusLine.statusCode).willReturn(201)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpGet::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.batchDataKey()

        val expectedResult: DataKeyResult = Gson().fromJson(responseBody, DataKeyResult::class.java)
        assertEquals(expectedResult, dataKeyResult)

        val argumentCaptor = ArgumentCaptor.forClass(HttpGet::class.java)
        verify(httpClient, times(1)).execute(argumentCaptor.capture())
        assertEquals("dummy.com:8090/datakey?correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
    }

    @Test
    fun testBatchDataKey_ServerError_ThrowsException_AndWillRetry() {
        val httpClient = mock(CloseableHttpClient::class.java)
        val statusLine = mock(StatusLine::class.java)
        //val entity = mock(HttpEntity::class.java)
        given(statusLine.statusCode).willReturn(503)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpClient.execute(any(HttpGet::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.batchDataKey()
            fail("Should throw a DataKeyServiceUnavailableException")
        } catch (ex: DataKeyServiceUnavailableException) {
            assertEquals("Getting batch data key - data key service returned bad status code '503' for dks_correlation_id: '$dksCallId'", ex.message)
            val argumentCaptor = ArgumentCaptor.forClass(HttpGet::class.java)
            verify(httpClient, times(5)).execute(argumentCaptor.capture())
            assertEquals("dummy.com:8090/datakey?correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @Test
    @Throws(DataKeyServiceUnavailableException::class)
    fun testBatchDataKey_UnknownHttpError_ThrowsException_AndWillRetry() {
        val statusLine = mock(StatusLine::class.java)
        //val entity = mock(HttpEntity::class.java)
        given(statusLine.statusCode).willReturn(503)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpGet::class.java))).willThrow(RuntimeException("Boom!"))
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.batchDataKey()
            fail("Should throw a DataKeyServiceUnavailableException")
        } catch (ex: DataKeyServiceUnavailableException) {
            assertEquals("Error contacting data key service: 'java.lang.RuntimeException: Boom!' for dks_correlation_id: '$dksCallId'", ex.message)
            val argumentCaptor = ArgumentCaptor.forClass(HttpGet::class.java)
            verify(httpClient, times(5)).execute(argumentCaptor.capture())
            assertEquals("dummy.com:8090/datakey?correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @Test
    @Throws(DataKeyServiceUnavailableException::class)
    fun testBatchDataKey_WhenErrorsOccur_WillRetryUntilSuccessful() {
        val responseBody = """
            |{
            |    "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
            |    "plaintextDataKey": "PLAINTEXT_DATAKEY",
            |    "ciphertextDataKey": "CIPHERTEXT_DATAKEY"
            |}
        """.trimMargin()

        val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
        val statusLine = mock(StatusLine::class.java)
        val entity = mock(HttpEntity::class.java)
        given(entity.content).willReturn(byteArrayInputStream)
        given(statusLine.statusCode).willReturn(503, 503, 201)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpGet::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.batchDataKey()

        val expectedResult: DataKeyResult = Gson().fromJson(responseBody, DataKeyResult::class.java)
        assertEquals(expectedResult, dataKeyResult)

        val argumentCaptor = ArgumentCaptor.forClass(HttpGet::class.java)
        verify(httpClient, times(3)).execute(argumentCaptor.capture())
        assertEquals("dummy.com:8090/datakey?correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
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
        val statusLine = mock(StatusLine::class.java)
        val entity = mock(HttpEntity::class.java)
        given(entity.content).willReturn(byteArrayInputStream)
        given(statusLine.statusCode).willReturn(200)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpPost::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")

        assertEquals("PLAINTEXT_DATAKEY", dataKeyResult)
        val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
        verify(httpClient, times(1)).execute(argumentCaptor.capture())
        assertEquals("dummy.com:8090/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
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
        val mockStatusLine = mock(StatusLine::class.java)
        val entity = mock(HttpEntity::class.java)
        given(entity.content).willReturn(byteArrayInputStream)
        given(mockStatusLine.statusCode).willReturn(503, 503, 200)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(mockStatusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpPost::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")

        assertEquals("PLAINTEXT_DATAKEY", dataKeyResult)

        val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
        verify(httpClient, times(3)).execute(argumentCaptor.capture())
        assertEquals("dummy.com:8090/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
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
        val statusLine = mock(StatusLine::class.java)
        val entity = mock(HttpEntity::class.java)
        given(entity.content).willReturn(byteArrayInputStream)
        given(statusLine.statusCode).willReturn(200)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpPost::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
        assertEquals("PLAINTEXT_DATAKEY", dataKeyResult)

        keyService.decryptKey("123", "ENCRYPTED_KEY_ID")

        val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
        verify(httpClient, times(1)).execute(argumentCaptor.capture())
        assertEquals("dummy.com:8090/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
    }

    @Test
    fun testDecryptKey_WithABadKey_WillCallServerOnce_AndNotRetry() {
        val statusLine = mock(StatusLine::class.java)
        given(statusLine.statusCode).willReturn(400)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpPost::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
            fail("Should throw a DataKeyDecryptionException")
        } catch (ex: DataKeyDecryptionException) {
            assertEquals("Decrypting encryptedKey: 'ENCRYPTED_KEY_ID' with keyEncryptionKeyId: '123' data key service returned status code '400' for dks_correlation_id: '$dksCallId'", ex.message)
            val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
            verify(httpClient, times(1)).execute(argumentCaptor.capture())
            assertEquals("dummy.com:8090/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @Test
    fun testDecryptKey_ServerError_WillCauseRetryMaxTimes() {
        val statusLine = mock(StatusLine::class.java)
        given(statusLine.statusCode).willReturn(503)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpPost::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
            fail("Should throw a DataKeyServiceUnavailableException")
        } catch (ex: DataKeyServiceUnavailableException) {
            assertEquals("Decrypting encryptedKey: 'ENCRYPTED_KEY_ID' with keyEncryptionKeyId: '123' data key service returned status code '503' for dks_correlation_id: '$dksCallId'", ex.message)
            val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
            verify(httpClient, times(5)).execute(argumentCaptor.capture())
            assertEquals("dummy.com:8090/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @Test
    fun testDecryptKey_UnknownHttpError_WillCauseRetryMaxTimes() {
        val statusLine = mock(StatusLine::class.java)
        given(statusLine.statusCode).willReturn(503)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpPost::class.java))).willThrow(RuntimeException("Boom!"))
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
            fail("Should throw a DataKeyServiceUnavailableException")
        } catch (ex: DataKeyServiceUnavailableException) {
            assertEquals("Error contacting data key service: 'java.lang.RuntimeException: Boom!' for dks_correlation_id: '$dksCallId'", ex.message)
            val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
            verify(httpClient, times(5)).execute(argumentCaptor.capture())
            assertEquals("dummy.com:8090/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @MockBean
    private lateinit var amazonDynamoDb: AmazonDynamoDB

    @MockBean
    private lateinit var amazonSQS: AmazonSQS


}
