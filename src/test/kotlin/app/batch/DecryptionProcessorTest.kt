package app.batch

import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.DataKeyDecryptionException
import app.exceptions.DataKeyServiceUnavailableException
import app.exceptions.DecryptionFailureException
import app.services.KeyService
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.BDDMockito.given
import org.mockito.Mockito
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@ActiveProfiles("decryptionTest", "aesCipherService", "unitTest", "outputToConsole")
@SpringBootTest
@TestPropertySource(properties = ["source.table.name=ucfs-data", "column.family=topic"])
class DecryptionProcessorTest {


    @Before
    fun init() = Mockito.reset(dataKeyService)

    @Test(expected = DataKeyServiceUnavailableException::class)
    fun testDataKeyServiceUnavailable() {
        given(dataKeyService.decryptKey(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .willThrow(DataKeyServiceUnavailableException::class.java)
        val encryptionBlock: EncryptionBlock =
                EncryptionBlock("encryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")

        val sourceRecord = SourceRecord("00001", 10, encryptionBlock, "dbObject")
        decryptionProcessor.process(sourceRecord)
    }


    @Test(expected = DecryptionFailureException::class)
    fun testDataKeyDecryptionFailure() {
        given(dataKeyService.decryptKey(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .willThrow(DataKeyDecryptionException::class.java)

        val encryptionBlock: EncryptionBlock =
                EncryptionBlock("encryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")
        decryptionProcessor.process(SourceRecord("00001", 10, encryptionBlock, "dbObject"))
    }

    @Autowired
    private lateinit var dataKeyService: KeyService

    @Autowired
    private lateinit var decryptionProcessor: DecryptionProcessor

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DirectoryWriterChunkingTest::class.toString())
    }
}