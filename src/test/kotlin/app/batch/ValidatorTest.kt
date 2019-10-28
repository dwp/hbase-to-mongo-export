package app.batch

import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.BadDecryptedDataException
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import org.junit.Assert.assertNotNull
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.junit4.SpringRunner
import java.nio.ByteBuffer
import java.util.zip.CRC32

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [Validator::class])
class ValidatorTest {

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
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
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
        val jsonObject = validator.parseDecrypted(String(generateFourByteChecksum("00001")), decryptedDbObject)
        val idJsonObject = validator.retrieveId(String(generateFourByteChecksum("00001")), jsonObject!!)
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
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00001' in db 'db' in collection 'collection' with the reason 'id not found in the decrypted db object'"
    }

    @Test
    fun Should_Retrieve_LastupdatedTimestamp_If_DbObject_Is_A_Valid_Json() {
        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {
                        "${'$'}date": "2018-12-14T15:01:02.000+0000"
                    }
                }"""
        val jsonObject = validator.parseDecrypted(String(generateFourByteChecksum("00001")), decryptedDbObject)
        val idJsonObject = validator.retrievelastUpdatedTimestamp(String(generateFourByteChecksum("00001")), jsonObject!!)
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
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00001' in db 'db' in collection 'collection' with the reason '_lastModifiedDateTime not found in the decrypted db object'"
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
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00001' in db 'db' in collection 'collection' with the reason '\$date in _lastModifiedDateTime not found in the decrypted db object'"
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
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00001' in db 'db' in collection 'collection' with the reason 'Unparseable date: \"\"2018-12-14\"\"'"
    }

    private fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()
        checksum.update(bytes, 0, bytes.size)
        val checksumBytes = ByteBuffer.allocate(4).putInt(checksum.getValue().toInt()).array();
        return checksumBytes.plus(bytes)
    }

    @SpyBean
    private lateinit var validator: Validator

}

