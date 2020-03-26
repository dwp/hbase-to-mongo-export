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
    fun Should_Process_If_Decrypted_DbObject_Is_A_Valid_Json() {
        val decryptedDbObject = """{"_id": {"someId": "RANDOM_GUID", "declarationId": 1234}, "type": "addressDeclaration", "contractId": 1234, "addressNumber": {"type": "AddressLine", "cryptoId": 1234}, "addressLine2": null, "townCity": {"type": "AddressLine", "cryptoId": 1234}, "postcode": "SM5 2LE", "processId": 1234, "effectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "paymentEffectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "createdDateTime": {"${"$"}date": "2015-03-20T12:23:25.183Z", "_archivedDateTime": "should be replaced by _archivedDateTime"}, "_version": 2, "_archived": "should be replaced by _removed", "unicodeNull": "\u0000", "unicodeNullwithText": "some\u0000text", "lineFeedChar": "\n", "lineFeedCharWithText": "some\ntext", "carriageReturn": "\r", "carriageReturnWithText": "some\rtext", "carriageReturnLineFeed": "\r\n", "carriageReturnLineFeedWithText": "some\r\ntext", "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000"}"""
        val lastModified = "2019-07-04T07:27:35.104+0000"
        val encryptionBlock: EncryptionBlock =
            EncryptionBlock("keyEncryptionKeyId","initialisationVector","encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"),
                10, encryptionBlock, "dbObject", "db", "collection", lastModified, "HDI")
        val decrypted = validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        assertNotNull(decrypted)
    }

    @Test
    fun Should_Process_If_Decrypted_DbObject_Is_A_Valid_Json_with_primitive_id() {
        val decryptedDbObject = """{"_id": "I am a string", "type": "addressDeclaration", "contractId": 1234, "addressNumber": {"type": "AddressLine", "cryptoId": 1234}, "addressLine2": null, "townCity": {"type": "AddressLine", "cryptoId": 1234}, "postcode": "SM5 2LE", "processId": 1234, "effectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "paymentEffectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "createdDateTime": {"${"$"}date": "2015-03-20T12:23:25.183Z", "_archivedDateTime": "should be replaced by _archivedDateTime"}, "_version": 2, "_archived": "should be replaced by _removed", "unicodeNull": "\u0000", "unicodeNullwithText": "some\u0000text", "lineFeedChar": "\n", "lineFeedCharWithText": "some\ntext", "carriageReturn": "\r", "carriageReturnWithText": "some\rtext", "carriageReturnLineFeed": "\r\n", "carriageReturnLineFeedWithText": "some\r\ntext", "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000"}"""
        val lastModified = "2019-07-04T07:27:35.104+0000"
        val encryptionBlock: EncryptionBlock =
                EncryptionBlock("keyEncryptionKeyId","initialisationVector","encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"),
                10, encryptionBlock, "dbObject", "db", "collection", lastModified, "HDI")
        val decrypted = validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        assertNotNull(decrypted)
    }

    @Test
    fun Should_Log_Error_If_Decrypted_DbObject_Is_A_InValid_Json() {
        val decryptedDbObject = "{\"testOne\":\"test1\", \"testTwo\":2"

        val lastModified = "2019-07-04T07:27:35.104+0000"
        val encryptionBlock: EncryptionBlock =
            EncryptionBlock("keyEncryptionKeyId",
                "initialisationVector",
                "encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00003"), 10, encryptionBlock,
                "dbObject", "db", "collection", lastModified, "HDI")
        val exception = shouldThrow<BadDecryptedDataException> {
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00003' in db 'db' in collection 'collection' with the reason 'java.io.EOFException: End of input at line 1 column 32 path \$.testTwo'"
    }

    @Test
    fun Should_Log_Error_If_Decrypted_DbObject_Is_A_JsonPrimitive() {
        val decryptedDbObject = "hello"

        val lastModified = "2019-07-04T07:27:35.104+0000"
        val encryptionBlock: EncryptionBlock =
                EncryptionBlock("keyEncryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00003"), 10, encryptionBlock,
                "dbObject", "db", "collection", lastModified, "HDI")
        val exception = shouldThrow<BadDecryptedDataException> {
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00003' in db 'db' in collection 'collection' with the reason 'Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive'"
    }

    @Test
    fun Should_Retrieve_ID_If_DbObject_Is_A_Valid_Json() {
        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000"
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val idJsonObject = validator.retrieveId(jsonObject!!)
        assertNotNull(idJsonObject)
    }

    @Test
    fun Should_Log_Error_If_DbObject_Doesnt_Have_Id() {
        val encryptionBlock: EncryptionBlock =
            EncryptionBlock("keyEncryptionKeyId",
                "initialisationVector",
                "encryptedEncryptionKey")
        val lastModified = "2019-07-04T07:27:35.104+0000"
        val decryptedDbObject = """{
                   "_id1":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000"
                }"""
        val sourceRecord = SourceRecord(generateFourByteChecksum("00002"), 10, encryptionBlock,
                "dbObject", "db", "collection",
                lastModified, "HDI")
        val exception = shouldThrow<BadDecryptedDataException> {
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00002' in db 'db' in collection 'collection' with the reason '_id field not found in the decrypted db object'"
    }



    private fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()
        checksum.update(bytes, 0, bytes.size)
        val checksumBytes = ByteBuffer.allocate(4).putInt(checksum.getValue().toInt()).array()
        return checksumBytes.plus(bytes)
    }

    @SpyBean
    private lateinit var validator: Validator

}

