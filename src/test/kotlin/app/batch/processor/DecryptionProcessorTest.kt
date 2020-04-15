package app.batch.processor

import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.DataKeyDecryptionException
import app.exceptions.DataKeyServiceUnavailableException
import app.exceptions.DecryptionFailureException
import app.services.KeyService
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
@ActiveProfiles("decryptionTest", "aesCipherService", "unitTest", "outputToConsole")
@SpringBootTest
@TestPropertySource(properties = [
    "data.table.name=ucfs-data",
    "column.family=topic",
    "topic.name=db.a.b",
    "identity.keystore=resources/identity.jks",
    "trust.keystore=resources/truststore.jks",
    "identity.store.password=changeit",
    "identity.key.password=changeit",
    "trust.store.password=changeit",
    "identity.store.alias=cid",
    "hbase.zookeeper.quorum=hbase",
    "aws.region=eu-west-2"
])
class DecryptionProcessorTest {

    @Before
    fun init() = Mockito.reset(dataKeyService)

    @Test(expected = DataKeyServiceUnavailableException::class)
    fun testDataKeyServiceUnavailable() {
        val lastModified = "2019-07-04T07:27:35.104+0000"

        given(dataKeyService.decryptKey(anyString(), anyString()))
            .willThrow(DataKeyServiceUnavailableException::class.java)
        val encryptionBlock: EncryptionBlock =
            EncryptionBlock("keyEncryptionKeyId",
                "initialisationVector",
                "encryptedEncryptionKey")

        val sourceRecord = SourceRecord("00001".toByteArray(), 10, encryptionBlock,
                "dbObject", "db", "collection", "HDI")
        decryptionProcessor.process(sourceRecord)
    }

    @Test(expected = DecryptionFailureException::class)
    fun testDataKeyDecryptionFailure() {
        val lastModified = "2019-07-04T07:27:35.104+0000"
        given(dataKeyService.decryptKey(anyString(), anyString()))
            .willThrow(DataKeyDecryptionException::class.java)

        val encryptionBlock: EncryptionBlock =
            EncryptionBlock("keyEncryptionKeyId",
                "initialisationVector",
                "encryptedEncryptionKey")
        decryptionProcessor.process(SourceRecord("00001".toByteArray(), 10, encryptionBlock,
                "dbObject", "db", "collection", "HDI"))
    }

    @MockBean
    private lateinit var dataKeyService: KeyService

    @SpyBean
    private lateinit var decryptionProcessor: DecryptionProcessor

}

