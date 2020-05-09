package app.batch

import app.domain.EncryptionBlock
import app.domain.ManifestRecord
import app.domain.SourceRecord
import app.exceptions.BadDecryptedDataException
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import org.junit.Assert.assertEquals
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
        val id = """{"someId":"RANDOM_GUID","declarationId":1234}"""
        val id_sorted = """{"declarationId":1234,"someId":"RANDOM_GUID"}"""
        val decryptedDbObject = """{"_id": $id, "type": "addressDeclaration", "contractId": 1234, "addressNumber": {"type": "AddressLine", "cryptoId": 1234}, "addressLine2": null, "townCity": {"type": "AddressLine", "cryptoId": 1234}, "postcode": "SM5 2LE", "processId": 1234, "effectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "paymentEffectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "createdDateTime": {"${"$"}date": "2015-03-20T12:23:25.183Z", "_archivedDateTime": "should be replaced by _archivedDateTime"}, "_version": 2, "_archived": "should be replaced by _removed", "unicodeNull": "\u0000", "unicodeNullwithText": "some\u0000text", "lineFeedChar": "\n", "lineFeedCharWithText": "some\ntext", "carriageReturn": "\r", "carriageReturnWithText": "some\rtext", "carriageReturnLineFeed": "\r\n", "carriageReturnLineFeedWithText": "some\r\ntext", "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000"}"""
        val encryptionBlock: EncryptionBlock =
            EncryptionBlock("keyEncryptionKeyId","initialisationVector","encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"),
                10, encryptionBlock, "dbObject", "db", "collection", "OUTER_TYPE", "INNER_TYPE")
        val decrypted = validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        assertNotNull(decrypted)
        assertNotNull(decrypted?.manifestRecord)
        val manifest = decrypted?.manifestRecord
        val expectedManifest = ManifestRecord(id_sorted, 1562225255104, "db", "collection", "EXPORT", "OUTER_TYPE", "INNER_TYPE", id_sorted)
        assertEquals(expectedManifest, manifest)
    }

    @Test
    fun Should_Process_If_Decrypted_DbObject_Is_A_Valid_Json_With_Primitive_Id() {
        val id = "JSON_PRIMITIVE_STRING"
        val decryptedDbObject = """{"_id": $id, "type": "addressDeclaration", "contractId": 1234, "addressNumber": {"type": "AddressLine", "cryptoId": 1234}, "addressLine2": null, "townCity": {"type": "AddressLine", "cryptoId": 1234}, "postcode": "SM5 2LE", "processId": 1234, "effectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "paymentEffectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "createdDateTime": {"${"$"}date": "2015-03-20T12:23:25.183Z", "_archivedDateTime": "should be replaced by _archivedDateTime"}, "_version": 2, "_archived": "should be replaced by _removed", "unicodeNull": "\u0000", "unicodeNullwithText": "some\u0000text", "lineFeedChar": "\n", "lineFeedCharWithText": "some\ntext", "carriageReturn": "\r", "carriageReturnWithText": "some\rtext", "carriageReturnLineFeed": "\r\n", "carriageReturnLineFeedWithText": "some\r\ntext", "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000"}"""
        val encryptionBlock: EncryptionBlock =
                EncryptionBlock("keyEncryptionKeyId","initialisationVector","encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"),
                10, encryptionBlock, "dbObject", "db", "collection", "OUTER_TYPE", "INNER_TYPE")
        val decrypted = validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        assertNotNull(decrypted)
        assertNotNull(decrypted?.manifestRecord)
        val manifest = decrypted?.manifestRecord
        val oid = "\$oid"
        val expectedManifest = ManifestRecord("""{"$oid":"$id"}""", 1562225255104, "db", "collection", "EXPORT", "OUTER_TYPE", "INNER_TYPE", id)
        assertEquals(expectedManifest, manifest)
    }

    @Test
    fun Should_Process_If_Decrypted_DbObject_Is_A_Valid_Json_With_Object_Id() {
        val id = """{"someId":"RANDOM_GUID","declarationId":1234}"""
        val id_sorted = """{"declarationId":1234,"someId":"RANDOM_GUID"}"""
        val decryptedDbObject = """{"_id": $id, "type": "addressDeclaration", "contractId": 1234, "addressNumber": {"type": "AddressLine", "cryptoId": 1234}, "addressLine2": null, "townCity": {"type": "AddressLine", "cryptoId": 1234}, "postcode": "SM5 2LE", "processId": 1234, "effectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "paymentEffectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "createdDateTime": {"${"$"}date": "2015-03-20T12:23:25.183Z", "_archivedDateTime": "should be replaced by _archivedDateTime"}, "_version": 2, "_archived": "should be replaced by _removed", "unicodeNull": "\u0000", "unicodeNullwithText": "some\u0000text", "lineFeedChar": "\n", "lineFeedCharWithText": "some\ntext", "carriageReturn": "\r", "carriageReturnWithText": "some\rtext", "carriageReturnLineFeed": "\r\n", "carriageReturnLineFeedWithText": "some\r\ntext", "_lastModifiedDateTime": {"${"$"}date": "2019-07-04T07:27:35.104+0000"}}"""
        val encryptionBlock: EncryptionBlock =
            EncryptionBlock("keyEncryptionKeyId","initialisationVector","encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"),
                10, encryptionBlock, "dbObject", "db", "collection", "OUTER_TYPE", "INNER_TYPE")
        val decrypted = validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        assertNotNull(decrypted)
        assertNotNull(decrypted?.manifestRecord)
        val manifest = decrypted?.manifestRecord
        val expectedManifest = ManifestRecord(id_sorted, 1562225255104, "db", "collection", "EXPORT", "OUTER_TYPE", "INNER_TYPE", id_sorted)
        assertEquals(expectedManifest, manifest)
    }

    @Test
    fun Should_Log_Error_If_Decrypted_DbObject_Is_A_InValid_Json() {
        val decryptedDbObject = "{\"testOne\":\"test1\", \"testTwo\":2"

        val encryptionBlock: EncryptionBlock =
            EncryptionBlock("keyEncryptionKeyId",
                "initialisationVector",
                "encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00003"), 10, encryptionBlock,
                "dbObject", "db", "collection", "OUTER_TYPE", "INNER_TYPE")
        val exception = shouldThrow<BadDecryptedDataException> {
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00003' in db 'db' in collection 'collection' with the reason 'java.io.EOFException: End of input at line 1 column 32 path \$.testTwo'"
    }

    @Test
    fun Should_Log_Error_If_Decrypted_DbObject_Is_A_JsonPrimitive() {
        val decryptedDbObject = "hello"

        val encryptionBlock: EncryptionBlock =
                EncryptionBlock("keyEncryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00003"), 10, encryptionBlock,
                "dbObject", "db", "collection", "OUTER_TYPE", "INNER_TYPE")
        val exception = shouldThrow<BadDecryptedDataException> {
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00003' in db 'db' in collection 'collection' with the reason 'Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive'"
    }

    @Test
    fun Should_Retrieve_ID_If_DbObject_Is_A_Valid_Json() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val dateTwo = "2018-12-14T15:01:02.000+0000" 

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "$dateOne",
                   "createdDateTime": "$dateTwo"
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val idJsonObject = validator.retrieveId(jsonObject!!)
        assertNotNull(idJsonObject)
    }

    @Test
    fun Should_Retrieve_LastModifiedDateTime_If_DbObject_Is_A_Valid_String() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val dateTwo = "2018-12-14T15:01:02.000+0000" 

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "$dateOne",
                   "createdDateTime": "$dateTwo"
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject!!)
        assertEquals(dateOne, actual)
    }

    @Test
    fun Should_Retrieve_LastModifiedDateTime_If_DbObject_Is_A_Valid_Json_Object() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val dateTwo = "2018-12-14T15:01:02.000+0000" 

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {"${"$"}date": "$dateOne"},
                   "createdDateTime": {"${"$"}date": "$dateTwo"}
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject!!)
        assertEquals(dateOne, actual)
    }

    @Test
    fun Should_Retrieve_CreatedDateTime_When_Present_And_LastModifiedDateTime_Is_Missing() {
        val dateOne = "2019-12-14T15:01:02.000+0000"

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "createdDateTime": "$dateOne"
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject!!)
        assertEquals(dateOne, actual)
    }

    @Test
    fun Should_Retrieve_CreatedDateTime_When_Present_And_LastModifiedDateTime_Is_An_InValid_Json_Object() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val dateTwo = "2018-12-14T15:01:02.000+0000" 

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {"date": "$dateOne"},
                   "createdDateTime": {"${"$"}date": "$dateTwo"}
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject!!)
        assertEquals(dateTwo, actual)
    }

    @Test
    fun Should_Retrieve_CreatedDateTime_When_Present_And_LastModifiedDateTime_Is_Empty() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        
        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "",
                   "createdDateTime": {"${"$"}date": "$dateOne"}
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject!!)
        assertEquals(dateOne, actual)
    }

    @Test
    fun Should_Retrieve_CreatedDateTime_When_Present_And_LastModifiedDateTime_Is_Null() {
        val dateOne = "2019-12-14T15:01:02.000+0000"

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": null,
                   "createdDateTime": "$dateOne"
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject!!)
        assertEquals(dateOne, actual)
    }

    @Test
    fun Should_Retrieve_Epoch_When_CreatedDateTime_And_LastModifiedDateTime_Are_Missing() {
        val epoch = "1980-01-01T00:00:00.000Z" 

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"}
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject!!)
        assertEquals(epoch, actual)
    }

    @Test
    fun Should_Retrieve_Epoch_When_CreatedDateTime_And_LastModifiedDateTime_Are_InValid_Json_Objects() {
        val epoch = "1980-01-01T00:00:00.000Z" 
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val dateTwo = "2018-12-14T15:01:02.000+0000" 

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {"date": "$dateOne"},
                   "createdDateTime": {"date": "$dateTwo"}
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject!!)
        assertEquals(epoch, actual)
    }

    @Test
    fun Should_Retrieve_Epoch_When_CreatedDateTime_And_LastModifiedDateTime_Are_Empty() {
        val epoch = "1980-01-01T00:00:00.000Z" 

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "",
                   "createdDateTime": ""
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject!!)
        assertEquals(epoch, actual)
    }

    @Test
    fun Should_Retrieve_Epoch_When_CreatedDateTime_And_LastModifiedDateTime_Are_Null() {
        val epoch = "1980-01-01T00:00:00.000Z" 

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": null,
                   "createdDateTime": null
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject!!)
        assertEquals(epoch, actual)
    }

    @Test
    fun Should_Retrieve_Value_String_When_Date_Element_Is_String() {
        val expected = "A Date" 

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "dateTimeTestElement": "$expected"
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveDateTimeElement("dateTimeTestElement", jsonObject!!)
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Retrieve_Value_String_When_Date_Element_Is_Valid_Object() {
        val expected = "A Date" 

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "dateTimeTestElement": {"${"$"}date": "$expected"}
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveDateTimeElement("dateTimeTestElement", jsonObject!!)
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Retrieve_Empty_String_When_Date_Element_Is_InValid_Object() {
        val expected = "" 

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "dateTimeTestElement": {"date": "$expected"}
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveDateTimeElement("dateTimeTestElement", jsonObject!!)
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Retrieve_Empty_String_When_Date_Element_Is_Null() {
        val expected = "" 

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "dateTimeTestElement": null
                }"""
        val jsonObject = validator.parseDecrypted(decryptedDbObject)
        val actual = validator.retrieveDateTimeElement("dateTimeTestElement", jsonObject!!)
        assertEquals(expected, actual)
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
        val sourceRecord = SourceRecord(generateFourByteChecksum("00002"), 10, encryptionBlock,
                "dbObject", "db", "collection",
                "OUTER_TYPE", "INNER_TYPE")
        val exception = shouldThrow<BadDecryptedDataException> {
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00002' in db 'db' in collection 'collection' with the reason '_id field not found in the decrypted db object'"
    }

    @Test
    fun Should_Replace_Value_When_Current_Value_Is_String() {
        val oldDate = "2019-12-14T15:01:02.000+0000"
        val newDate = "2018-12-14T15:01:02.000+0000" 

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "$oldDate"
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$newDate"}
                }"""

        val oldJsonObject = validator.parseDecrypted(oldJson)
        val expected = validator.parseDecrypted(newJson)
        val actual = validator.replaceElementValueWithKeyValuePair(oldJsonObject!!, "_lastModifiedDateTime", "${"$"}date", newDate)
        
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Replace_Value_When_Current_Value_Is_Object_With_Matching_Key() {
        val oldDate = "2019-12-14T15:01:02.000+0000"
        val newDate = "2018-12-14T15:01:02.000+0000" 

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {"${"$"}date": "$oldDate"}
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$newDate"}
                }"""

        val oldJsonObject = validator.parseDecrypted(oldJson)
        val expected = validator.parseDecrypted(newJson)
        val actual = validator.replaceElementValueWithKeyValuePair(oldJsonObject!!, "_lastModifiedDateTime", "${"$"}date", newDate)
        
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Replace_Value_When_Current_Value_Is_Object_With_No_Matching_Key() {
        val oldDate = "2019-12-14T15:01:02.000+0000"
        val newDate = "2018-12-14T15:01:02.000+0000" 

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {"notDate": "$oldDate"}
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$newDate"}
                }"""

        val oldJsonObject = validator.parseDecrypted(oldJson)
        val expected = validator.parseDecrypted(newJson)
        val actual = validator.replaceElementValueWithKeyValuePair(oldJsonObject!!, "_lastModifiedDateTime", "${"$"}date", newDate)
        
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Replace_Value_When_Current_Value_Is_Object_With_No_Multiple_Keys() {
        val oldDate = "2019-12-14T15:01:02.000+0000"
        val newDate = "2018-12-14T15:01:02.000+0000" 

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {"notDate": "$oldDate", "notDateTwo": "$oldDate"}
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$newDate"}
                }"""

        val oldJsonObject = validator.parseDecrypted(oldJson)
        val expected = validator.parseDecrypted(newJson)
        val actual = validator.replaceElementValueWithKeyValuePair(oldJsonObject!!, "_lastModifiedDateTime", "${"$"}date", newDate)
        
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Wrap_All_Dates() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val dateTwo = "2018-12-14T15:01:02.000+0000" 
        val dateThree = "2017-12-14T15:01:02.000+0000" 

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "$dateOne",
                   "createdDateTime": "$dateTwo",
                   "_removedDateTime": "$dateThree"
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$dateOne"},
                    "createdDateTime": {"${"$"}date": "$dateTwo"},
                    "_removedDateTime": {"${"$"}date": "$dateThree"}
                }"""

        val oldJsonObject = validator.parseDecrypted(oldJson)
        val expected = validator.parseDecrypted(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject!!)
        
        assertEquals(expected, actual)
        assertEquals(dateOne, lastModifiedDate)
    }

    @Test
    fun Should_Keep_Wrapped_Dates_Within_Wrapper() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val dateTwo = "2018-12-14T15:01:02.000+0000" 
        val dateThree = "2017-12-14T15:01:02.000+0000" 

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {"${"$"}date": "$dateOne"},
                   "createdDateTime": {"${"$"}date": "$dateTwo"},
                   "_removedDateTime": {"${"$"}date": "$dateThree"}
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$dateOne"},
                    "createdDateTime": {"${"$"}date": "$dateTwo"},
                    "_removedDateTime": {"${"$"}date": "$dateThree"}
                }"""

        val oldJsonObject = validator.parseDecrypted(oldJson)
        val expected = validator.parseDecrypted(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject!!)
        
        assertEquals(expected, actual)
        assertEquals(dateOne, lastModifiedDate)
    }

    @Test
    fun Should_Allow_For_Missing_Created_And_Removed_Dates() {
        val dateOne = "2019-12-14T15:01:02.000+0000"

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "$dateOne"
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$dateOne"}
                }"""

        val oldJsonObject = validator.parseDecrypted(oldJson)
        val expected = validator.parseDecrypted(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject!!)
        
        assertEquals(expected, actual)
        assertEquals(dateOne, lastModifiedDate)
    }

    @Test
    fun Should_Allow_For_Empty_Created_And_Removed_Dates() {
        val dateOne = "2019-12-14T15:01:02.000+0000"

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "$dateOne",
                   "createdDateTime": "",
                   "_removedDateTime": ""
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$dateOne"},
                    "createdDateTime": "",
                    "_removedDateTime": ""
                }"""

        val oldJsonObject = validator.parseDecrypted(oldJson)
        val expected = validator.parseDecrypted(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject!!)
        
        assertEquals(expected, actual)
        assertEquals(dateOne, lastModifiedDate)
    }

    @Test
    fun Should_Allow_For_Null_Created_And_Removed_Dates() {
        val dateOne = "2019-12-14T15:01:02.000+0000"

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "$dateOne",
                   "createdDateTime": null,
                   "_removedDateTime": null
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$dateOne"},
                    "createdDateTime": null,
                    "_removedDateTime": null
                }"""

        val oldJsonObject = validator.parseDecrypted(oldJson)
        val expected = validator.parseDecrypted(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject!!)
        
        assertEquals(expected, actual)
        assertEquals(dateOne, lastModifiedDate)
    }

    @Test
    fun Should_Create_Last_Modified_If_Missing_Dates() {
        val epoch = "1980-01-01T00:00:00.000Z" 

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"}
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$epoch"}
                }"""

        val oldJsonObject = validator.parseDecrypted(oldJson)
        val expected = validator.parseDecrypted(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject!!)
        
        assertEquals(expected, actual)
        assertEquals(epoch, lastModifiedDate)
    }

    @Test
    fun Should_Sort_Json_By_Key_Name() {
        val jsonStringUnsorted = "{\"testA\":\"test1\", \"testC\":2, \"testB\":true}"
        val jsonObjectUnsorted = validator.parseDecrypted(jsonStringUnsorted)
        val jsonStringSorted = "{\"testA\":\"test1\",\"testB\":true,\"testC\":2}"

        val sortedJson = validator.sortJsonByKey(jsonObjectUnsorted!!)

        sortedJson shouldBe jsonStringSorted
    }

    @Test
    fun Should_Sort_Json_By_Key_Name_Case_Sensitively() {
        val jsonStringUnsorted = "{\"testb\":true, \"testA\":\"test1\", \"testC\":2}"
        val jsonObjectUnsorted = validator.parseDecrypted(jsonStringUnsorted)
        val jsonStringSorted = "{\"testA\":\"test1\",\"testC\":2,\"testb\":true}"

        val sortedJson = validator.sortJsonByKey(jsonObjectUnsorted!!)

        sortedJson shouldBe jsonStringSorted
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

