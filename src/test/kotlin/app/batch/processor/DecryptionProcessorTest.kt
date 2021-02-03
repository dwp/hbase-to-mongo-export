package app.batch.processor

import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.DataKeyDecryptionException
import app.exceptions.DataKeyServiceUnavailableException
import app.exceptions.DecryptionFailureException
import app.services.KeyService
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.sqs.AmazonSQS
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers.anyString
import org.mockito.BDDMockito.given
import org.mockito.Mockito
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@ActiveProfiles("decryptionTest", "aesCipherService", "unitTest")
@SpringBootTest
@TestPropertySource(properties = [
    "hbase.zookeeper.quorum=hbase",
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
class DecryptionProcessorTest {

    @Before
    fun init() = Mockito.reset(dataKeyService)

    @Test(expected = DataKeyServiceUnavailableException::class)
    fun testDataKeyServiceUnavailable() {
        given(dataKeyService.decryptKey(anyString(), anyString()))
            .willThrow(DataKeyServiceUnavailableException::class.java)
        val encryptionBlock =
            EncryptionBlock("keyEncryptionKeyId",
                "initialisationVector",
                "encryptedEncryptionKey")

        val sourceRecord = SourceRecord("00001".toByteArray(), encryptionBlock,
                "dbObject", 100, "db", "collection", "OUTER_TYPE", "INNER_TYPE")
        decryptionProcessor.process(sourceRecord)
    }

    @Test(expected = DecryptionFailureException::class)
    fun testDataKeyDecryptionFailure() {
        given(dataKeyService.decryptKey(anyString(), anyString()))
            .willThrow(DataKeyDecryptionException::class.java)

        val encryptionBlock =
            EncryptionBlock("keyEncryptionKeyId",
                "initialisationVector",
                "encryptedEncryptionKey")
        decryptionProcessor.process(SourceRecord("00001".toByteArray(), encryptionBlock,
                "dbObject", 100, "db", "collection","OUTER_TYPE", "INNER_TYPE"))
    }

    @MockBean
    private lateinit var dataKeyService: KeyService

    @SpyBean
    private lateinit var decryptionProcessor: DecryptionProcessor

    @MockBean
    private lateinit var amazonDynamoDb: AmazonDynamoDB

    @MockBean
    private lateinit var amazonSQS: AmazonSQS
}

