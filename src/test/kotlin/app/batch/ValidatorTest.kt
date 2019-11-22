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
        val decryptedDbObject = """{"_id": {"someId": "RANDOM_GUID", "declarationId": 1234}, "type": "addressDeclaration", "contractId": 1234, "addressNumber": {"type": "AddressLine", "cryptoId": 1234}, "addressLine2": null, "townCity": {"type": "AddressLine", "cryptoId": 1234}, "postcode": "SM5 2LE", "processId": 1234, "effectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "paymentEffectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "createdDateTime": {"${"$"}date": "2015-03-20T12:23:25.183Z", "_archivedDateTime": "should be replaced by _archivedDateTime"}, "_version": 2, "_archived": "should be replaced by _removed", "unicodeNull": "\u0000", "unicodeNullwithText": "some\u0000text", "lineFeedChar": "\n", "lineFeedCharWithText": "some\ntext", "carriageReturn": "\r", "carriageReturnWithText": "some\rtext", "carriageReturnLineFeed": "\r\n", "carriageReturnLineFeedWithText": "some\r\ntext", "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000"}
"""

        val encryptionBlock: EncryptionBlock =
            EncryptionBlock("keyEncryptionKeyId",
                "initialisationVector",
                "encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"), 10, encryptionBlock, "dbObject", "db", "collection")
        val decrypted = validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        assertNotNull(decrypted)
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
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00001' in db 'db' in collection 'collection' with the reason 'Exception occurred while parsing decrypted db object'"
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
        val decryptedDbObject = """{
                   "_id1":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000"
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
                   "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000"
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val idJsonObject = validator.retrieveLastUpdatedTimestamp(jsonObject!!)
        assertNotNull(idJsonObject)
    }

    @Test
    fun Should_Retrieve_Timestamp_If_Timestamp_is_object() {
        val dateSubField = "\$date"
        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": { "$dateSubField": "2018-12-14T15:01:02.000+0000" }
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val idJsonObject = validator.retrieveLastUpdatedTimestamp(jsonObject!!)
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
                   "_lastModifiedDateTime1": "2018-12-14T15:01:02.000+0000"
                }"""
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"), 10, encryptionBlock, "dbObject", "db", "collection")
        val exception = shouldThrow<BadDecryptedDataException> {
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00001' in db 'db' in collection 'collection' with the reason ''_lastModifiedDateTime' field not found in the decrypted db object'"
    }

    @Test
    fun Should_Log_Error_If_lastModifiedDateTime_Date_Doesnt_Have_Valid_Format() {
        val encryptionBlock: EncryptionBlock =
            EncryptionBlock("keyEncryptionKeyId",
                "initialisationVector",
                "encryptedEncryptionKey")
        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "2018-12-14"
                }"""
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"), 10, encryptionBlock, "dbObject", "db", "collection")
        val exception = shouldThrow<BadDecryptedDataException> {
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00001' in db 'db' in collection 'collection' with the reason 'Unparseable date: \"2018-12-14\"'"
    }

    private fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()
        checksum.update(bytes, 0, bytes.size)
        val checksumBytes = ByteBuffer.allocate(4).putInt(checksum.getValue().toInt()).array();
        return checksumBytes.plus(bytes)
    }

    @Test
    fun Should_Retrieve_LastupdatedTimestamp_If_DbObject_Is_A_Valid_Json2() {
        val decryptedDbObject = "{\"_id\": {\"someId\": \"RANDOM_GUID\", \"declarationId\": 1234}, \"type\": \"addressDeclaration\", \"contractId\": 1234, \"addressNumber\": {\"type\": \"AddressLine\", \"cryptoId\": 1234}, \"addressLine2\": null, \"townCity\": {\"type\": \"AddressLine\", \"cryptoId\": 1234}, \"postcode\": \"SM5 2LE\", \"processId\": 1234, \"effectiveDate\": {\"type\": \"SPECIFIC_EFFECTIVE_DATE\", \"date\": 20150320, \"knownDate\": 20150320}, \"paymentEffectiveDate\": {\"type\": \"SPECIFIC_EFFECTIVE_DATE\", \"date\": 20150320, \"knownDate\": 20150320}, \"createdDateTime\": {\"\$date\": \"2015-03-20T12:23:25.183Z\", \"_archivedDateTime\": \"should be replaced by _archivedDateTime\"}, \"_version\": 2, \"_archived\": \"should be replaced by _removed\", \"unicodeNull\": \"\\u0000\", \"unicodeNullwithText\": \"some\\u0000text\", \"lineFeedChar\": \"\\n\", \"lineFeedCharWithText\": \"some\\ntext\", \"carriageReturn\": \"\\r\", \"carriageReturnWithText\": \"some\\rtext\", \"carriageReturnLineFeed\": \"\\r\\n\", \"carriageReturnLineFeedWithText\": \"some\\r\\ntext\", \"_lastModifiedDateTime\": \"2018-12-14T15:01:02.000+0000\"}\n"
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val idJsonObject = validator.retrieveLastUpdatedTimestamp(jsonObject!!)
        assertNotNull(idJsonObject)
    }

    @Test
    fun should_Convert_Timestamp_To_Long_With_Timezone() {
        val jsonString = "{\"_id\": {\"someId\": \"RANDOM_GUID\", \"declarationId\": 1234}, \"type\": \"addressDeclaration\", \"contractId\": 1234, \"addressNumber\": {\"type\": \"AddressLine\", \"cryptoId\": 1234}, \"addressLine2\": null, \"townCity\": {\"type\": \"AddressLine\", \"cryptoId\": 1234}, \"postcode\": \"SM5 2LE\", \"processId\": 1234, \"effectiveDate\": {\"type\": \"SPECIFIC_EFFECTIVE_DATE\", \"date\": 20150320, \"knownDate\": 20150320}, \"paymentEffectiveDate\": {\"type\": \"SPECIFIC_EFFECTIVE_DATE\", \"date\": 20150320, \"knownDate\": 20150320}, \"createdDateTime\": {\"\$date\": \"2015-03-20T12:23:25.183+0000\", \"_archivedDateTime\": \"should be replaced by _archivedDateTime\"}, \"_version\": 2, \"_archived\": \"should be replaced by _removed\", \"unicodeNull\": \"\\u0000\", \"unicodeNullwithText\": \"some\\u0000text\", \"lineFeedChar\": \"\\n\", \"lineFeedCharWithText\": \"some\\ntext\", \"carriageReturn\": \"\\r\", \"carriageReturnWithText\": \"some\\rtext\", \"carriageReturnLineFeed\": \"\\r\\n\", \"carriageReturnLineFeedWithText\": \"some\\r\\ntext\", \"_lastModifiedDateTime\": \"2018-12-14T15:01:02.000+0000\"}\n"
        val decrypted = validator.parseDecrypted(jsonString)
        val timestamp = validator.retrieveLastUpdatedTimestamp(decrypted!!)
        val timeAsLong = validator.validateTimestampFormat(timestamp!!)
        assertNotNull(timeAsLong)
    }

    @Test
    fun should_Convert_Timestamp_To_Long_Without_Timezone() {
        val jsonString = """{
            "_id": {"someId": "RANDOM_GUID", "declarationId": 1234}, 
            "type": "addressDeclaration", 
            "contractId": 1234, 
            "addressNumber": {
                "type": "AddressLine", "cryptoId": 1234
            }, 
            "addressLine2": null, 
            "townCity": {
                "type": "AddressLine", 
                "cryptoId": 1234
            }, 
            "postcode": "SM5 2LE", 
            "processId": 1234, 
            "effectiveDate": {
                "type": "SPECIFIC_EFFECTIVE_DATE", 
                "date": 20150320, 
                "knownDate": 20150320}, 
                "paymentEffectiveDate": {
                    "type": "SPECIFIC_EFFECTIVE_DATE", 
                    "date": 20150320, "knownDate": 20150320}, 
                    "createdDateTime": {
                        "${"$"}date": "2015-03-20T12:23:25.183Z", 
                        "_archivedDateTime": "should be replaced by _archivedDateTime"}, 
                        "_version": 2, 
                        "_archived": 
                        "should be replaced by _removed", "unicodeNull": "\u0000", "unicodeNullwithText": "some\u0000text", "lineFeedChar": "\n", "lineFeedCharWithText": "some\ntext", "carriageReturn": "\r", "carriageReturnWithText": "some\rtext", "carriageReturnLineFeed": "\r\n", "carriageReturnLineFeedWithText": "some\r\ntext", "_lastModifiedDateTime":"2018-12-14T15:01:02.000+0000"}
"""
        val decrypted = validator.parseDecrypted(jsonString)
        val timestamp = validator.retrieveLastUpdatedTimestamp(decrypted!!)
        val timeAsLong = validator.validateTimestampFormat(timestamp!!)
        assertNotNull(timeAsLong)
    }

    @SpyBean
    private lateinit var validator: Validator

}

