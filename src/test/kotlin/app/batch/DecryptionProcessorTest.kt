package app.batch

import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.DataKeyDecryptionException
import app.exceptions.DataKeyServiceUnavailableException
import app.exceptions.DecryptionFailureException
import app.services.CipherService
import app.services.KeyService
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import io.prometheus.client.Counter
import org.junit.Before
import org.junit.Test
import org.junit.jupiter.api.assertThrows
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers.anyString
import org.mockito.BDDMockito.given
import org.mockito.Mockito
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [DecryptionProcessor::class])
class DecryptionProcessorTest {

    @Before
    fun init() = Mockito.reset(dataKeyService)

    @Test
    fun testDataKeyServiceUnavailable() {
        given(dataKeyService.decryptKey(anyString(), anyString()))
            .willThrow(DataKeyServiceUnavailableException::class.java)
        val encryptionBlock =
            EncryptionBlock("keyEncryptionKeyId",
                "initialisationVector",
                "encryptedEncryptionKey")

        val sourceRecord = SourceRecord("00001".toByteArray(), encryptionBlock,
                "dbObject", 100, "db", "collection", "OUTER_TYPE", "INNER_TYPE")

        assertThrows<DataKeyServiceUnavailableException> {
            decryptionProcessor.process(sourceRecord)
        }

         verify(dksNewDataKeyFailuresCounter, times(1)).inc()

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
    private lateinit var cipherService: CipherService

    @MockBean
    private lateinit var validator: Validator

    @MockBean(name = "dksNewDataKeyFailuresCounter")
    private lateinit var dksNewDataKeyFailuresCounter: Counter
}

