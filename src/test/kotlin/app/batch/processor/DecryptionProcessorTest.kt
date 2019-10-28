package app.batch.processor

import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.BadDecryptedDataException
import app.exceptions.DataKeyDecryptionException
import app.exceptions.DataKeyServiceUnavailableException
import app.exceptions.DecryptionFailureException
import app.services.CipherService
import app.services.KeyService
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import org.junit.Assert.assertNotNull
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
import java.nio.ByteBuffer
import java.util.zip.CRC32

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
        given(dataKeyService.decryptKey(anyString(), anyString()))
                .willThrow(DataKeyServiceUnavailableException::class.java)
        val encryptionBlock: EncryptionBlock =
                EncryptionBlock("keyEncryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")

        val sourceRecord = SourceRecord("00001".toByteArray(), 10, encryptionBlock, "dbObject", "db", "collection")
        decryptionProcessor.process(sourceRecord)
    }

    @Test(expected = DecryptionFailureException::class)
    fun testDataKeyDecryptionFailure() {
        given(dataKeyService.decryptKey(anyString(), anyString()))
                .willThrow(DataKeyDecryptionException::class.java)

        val encryptionBlock: EncryptionBlock =
                EncryptionBlock("keyEncryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")
        decryptionProcessor.process(SourceRecord("00001".toByteArray(), 10, encryptionBlock, "dbObject", "db", "collection"))
    }

    @Test
    fun Should_Parse_If_Decrypted_DbObject_Is_A_Valid_Json() {
        val decryptedDbObject = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val jsonObject = decryptionProcessor.parseDecrypted(String(generateFourByteChecksum("00001")), decryptedDbObject)
        assertNotNull(jsonObject)
    }

    @Test
    fun Should_Log_Error_If_Decrypted_DbObject_Is_A_InValid_Json() {
        val decryptedDbObject = "{\"testOne\":\"test1\", \"testTwo\":2"
        "00001".toByteArray()

        val encryptionBlock: EncryptionBlock =
                EncryptionBlock("keyEncryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"), 10, encryptionBlock, "dbObject", "db", "collection")
        val exception = shouldThrow<BadDecryptedDataException> {
            decryptionProcessor.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00001' in db 'db' in collection 'collection' with the reason 'Exception occurred while parsing decrypted db object'"
    }

    @Test
    fun Should_Retrieve_ID_If_DbObject_Is_A_Valid_Json() {

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {
                        "${'$'}date": "2018-12-14T15:01:02.000+0000"
                    }
                }"""
        val jsonObject = decryptionProcessor.parseDecrypted(String(generateFourByteChecksum("00001")), decryptedDbObject)
        val idJsonObject = decryptionProcessor.retrieveId(String(generateFourByteChecksum("00001")), jsonObject!!)
        assertNotNull(idJsonObject)
    }

    @Test
    fun Should_Log_Error_If_DbObject_Doesnt_Have_Id() {
        val encryptionBlock: EncryptionBlock =
                EncryptionBlock("keyEncryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")
        val decryptedDbObject = """{
                   "_id1":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {
                        "${'$'}date": "2018-12-14T15:01:02.000+0000"
                    }
                }"""
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"), 10, encryptionBlock, "dbObject", "db", "collection")
        val exception = shouldThrow<BadDecryptedDataException> {
            decryptionProcessor.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00001' in db 'db' in collection 'collection' with the reason 'Exception : id not found in the decrypted db object'"
    }

    @Test
    fun Should_Retrieve_LastupdatedTimestamp_If_DbObject_Is_A_Valid_Json() {
        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {
                        "${'$'}date": "2018-12-14T15:01:02.000+0000"
                    }
                }"""
        val jsonObject = decryptionProcessor.parseDecrypted(String(generateFourByteChecksum("00001")), decryptedDbObject)
        val idJsonObject = decryptionProcessor.retrievelastUpdatedTimestamp(String(generateFourByteChecksum("00001")), jsonObject!!)
        assertNotNull(idJsonObject)
    }

    @Test
    fun Should_Log_Error_If_DbObject_Doesnt_Have_LastUpdatedTimestamp() {
        val encryptionBlock: EncryptionBlock =
                EncryptionBlock("keyEncryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")
        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime1": {
                        "${'$'}date": "2018-12-14T15:01:02.000+0000"
                    }
                }"""
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"), 10, encryptionBlock, "dbObject", "db", "collection")
        val exception = shouldThrow<BadDecryptedDataException> {
            decryptionProcessor.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00001' in db 'db' in collection 'collection' with the reason 'Exception : _lastModifiedDateTime not found in the decrypted db object'"
    }

    @Test
    fun Should_Log_Error_If_lastModifiedDateTime_Doesnt_Have_Dollar_Date() {
        val encryptionBlock: EncryptionBlock =
                EncryptionBlock("keyEncryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")
        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {
                        "${'#'}date": "2018-12-14T15:01:02.000+0000"
                    }
                }"""
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"), 10, encryptionBlock, "dbObject", "db", "collection")
        val exception = shouldThrow<BadDecryptedDataException> {
            decryptionProcessor.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00001' in db 'db' in collection 'collection' with the reason 'Exception : \$date in _lastModifiedDateTime not found in the decrypted db object'"
    }

    @Test
    fun Should_Log_Error_If_lastModifiedDateTime_Date_Doesnt_Have_Valid_Format() {
        val encryptionBlock: EncryptionBlock =
                EncryptionBlock("keyEncryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")
        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {
                        "${'$'}date": "2018-12-14"
                    }
                }"""
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"), 10, encryptionBlock, "dbObject", "db", "collection")
        val exception = shouldThrow<BadDecryptedDataException> {
            decryptionProcessor.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00001' in db 'db' in collection 'collection' with the reason 'Exception : Unparseable date: \"\"2018-12-14\"\"'"
    }

    private fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()
        checksum.update(bytes, 0, bytes.size)
        val checksumBytes = ByteBuffer.allocate(4).putInt(checksum.getValue().toInt()).array();
        return checksumBytes.plus(bytes)
    }

    @MockBean
    private lateinit var dataKeyService: KeyService

    @SpyBean
    private lateinit var decryptionProcessor: DecryptionProcessor

}

