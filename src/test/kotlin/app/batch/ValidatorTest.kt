package app.batch

import app.domain.DecryptedRecord
import app.domain.EncryptionBlock
import app.domain.ManifestRecord
import app.domain.SourceRecord
import app.exceptions.BadDecryptedDataException
import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import com.nhaarman.mockitokotlin2.*
import io.kotest.assertions.json.shouldMatchJson
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.prometheus.client.Counter
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.junit4.SpringRunner
import java.nio.ByteBuffer
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.zip.CRC32

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [Validator::class])
class ValidatorTest {

    @Before
    fun before() {
        reset(counter)
    }

    @Test
    fun Should_Process_If_Decrypted_DbObject_Is_A_Valid_Json() {
        val id = """{"someId":"RANDOM_GUID","declarationId":1234}"""
        val idSorted = """{"declarationId":1234,"someId":"RANDOM_GUID"}"""
        val decryptedDbObject = """{"_id": $id, "type": "addressDeclaration", "contractId": 1234, "addressNumber": {"type": "AddressLine", "cryptoId": 1234}, "addressLine2": null, "townCity": {"type": "AddressLine", "cryptoId": 1234}, "postcode": "SM5 2LE", "processId": 1234, "effectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "paymentEffectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "createdDateTime": {"${"$"}date": "2015-03-20T12:23:25.183Z", "_archivedDateTime": "should be replaced by _removedDateTime"}, "_version": 2, "_archived": "should be replaced by _removed", "unicodeNull": "\u0000", "unicodeNullwithText": "some\u0000text", "lineFeedChar": "\n", "lineFeedCharWithText": "some\ntext", "carriageReturn": "\r", "carriageReturnWithText": "some\rtext", "carriageReturnLineFeed": "\r\n", "carriageReturnLineFeedWithText": "some\r\ntext", "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000"}"""
        val encryptionBlock =
                EncryptionBlock("keyEncryptionKeyId", "initialisationVector", "encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"),
                encryptionBlock, "dbObject", 1000,"db", "collection", "OUTER_TYPE", "INNER_TYPE", "")
        val decrypted = validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        assertNotNull(decrypted)
        assertNotNull(decrypted?.manifestRecord)
        val manifest = decrypted?.manifestRecord
        val expectedManifest = ManifestRecord(idSorted, 1000, "db", "collection", "EXPORT", "OUTER_TYPE", "INNER_TYPE", idSorted)
        assertEquals(expectedManifest, manifest)
        verifyZeroInteractions(counter)
    }

    @Test
    fun Should_Process_If_Decrypted_DbObject_Is_A_Valid_Json_With_Primitive_Id() {
        val id = "JSON_PRIMITIVE_STRING"
        val decryptedDbObject = """{"_id": $id, "type": "addressDeclaration", "contractId": 1234, "addressNumber": {"type": "AddressLine", "cryptoId": 1234}, "addressLine2": null, "townCity": {"type": "AddressLine", "cryptoId": 1234}, "postcode": "SM5 2LE", "processId": 1234, "effectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "paymentEffectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "createdDateTime": {"${"$"}date": "2015-03-20T12:23:25.183Z", "_archivedDateTime": "should be replaced by _removedDateTime"}, "_version": 2, "_archived": "should be replaced by _removed", "unicodeNull": "\u0000", "unicodeNullwithText": "some\u0000text", "lineFeedChar": "\n", "lineFeedCharWithText": "some\ntext", "carriageReturn": "\r", "carriageReturnWithText": "some\rtext", "carriageReturnLineFeed": "\r\n", "carriageReturnLineFeedWithText": "some\r\ntext", "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000"}"""
        val encryptionBlock =
                EncryptionBlock("keyEncryptionKeyId", "initialisationVector", "encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"),
                encryptionBlock, "dbObject", 1000, "db", "collection", "OUTER_TYPE", "INNER_TYPE", "")
        val decrypted = validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        assertNotNull(decrypted)
        assertNotNull(decrypted?.manifestRecord)
        val manifest = decrypted?.manifestRecord
        val oid = "\$oid"
        val expectedManifest = ManifestRecord("""{"$oid":"$id"}""", 1000, "db", "collection", "EXPORT", "OUTER_TYPE", "INNER_TYPE", id)
        assertEquals(expectedManifest, manifest)
        verifyZeroInteractions(counter)
    }

    @Test
    fun Should_Process_If_Decrypted_DbObject_Is_A_Valid_Json_With_Object_Id() {
        val id = """{"someId":"RANDOM_GUID","declarationId":1234}"""
        val idSorted = """{"declarationId":1234,"someId":"RANDOM_GUID"}"""
        val decryptedDbObject = """{"_id": $id, "type": "addressDeclaration", "contractId": 1234, "addressNumber": {"type": "AddressLine", "cryptoId": 1234}, "addressLine2": null, "townCity": {"type": "AddressLine", "cryptoId": 1234}, "postcode": "SM5 2LE", "processId": 1234, "effectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "paymentEffectiveDate": {"type": "SPECIFIC_EFFECTIVE_DATE", "date": 20150320, "knownDate": 20150320}, "createdDateTime": {"${"$"}date": "2015-03-20T12:23:25.183Z", "_archivedDateTime": "should be replaced by _removedDateTime"}, "_version": 2, "_archived": "should be replaced by _removed", "unicodeNull": "\u0000", "unicodeNullwithText": "some\u0000text", "lineFeedChar": "\n", "lineFeedCharWithText": "some\ntext", "carriageReturn": "\r", "carriageReturnWithText": "some\rtext", "carriageReturnLineFeed": "\r\n", "carriageReturnLineFeedWithText": "some\r\ntext", "_lastModifiedDateTime": {"${"$"}date": "2019-07-04T07:27:35.104+0000"}}"""
        val encryptionBlock =
                EncryptionBlock("keyEncryptionKeyId", "initialisationVector", "encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"),
                encryptionBlock, "dbObject", 1000, "db", "collection", "OUTER_TYPE", "INNER_TYPE", "")
        val decrypted = validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        assertNotNull(decrypted)
        assertNotNull(decrypted?.manifestRecord)
        val manifest = decrypted?.manifestRecord
        val expectedManifest = ManifestRecord(idSorted, 1000, "db", "collection", "EXPORT", "OUTER_TYPE", "INNER_TYPE", idSorted)
        assertEquals(expectedManifest, manifest)
        verifyZeroInteractions(counter)
    }

    @Test
    fun Should_Log_Error_If_Decrypted_DbObject_Is_A_InValid_Json() {
        val decryptedDbObject = "{\"testOne\":\"test1\", \"testTwo\":2"

        val encryptionBlock =
                EncryptionBlock("keyEncryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00003"), encryptionBlock,
                "dbObject", 1000, "db", "collection", "OUTER_TYPE", "INNER_TYPE", "")
        val exception = shouldThrow<BadDecryptedDataException> {
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00003' in db 'db' in collection 'collection' with the reason 'java.io.EOFException: End of input at line 1 column 32 path \$.testTwo'"
        verify(counter, times(1)).inc()
        verifyNoMoreInteractions(counter)
    }

    @Test
    fun Should_Log_Error_If_Decrypted_DbObject_Is_A_JsonPrimitive() {
        val decryptedDbObject = "hello"

        val encryptionBlock =
                EncryptionBlock("keyEncryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00003"), encryptionBlock,
                "dbObject", 1000, "db", "collection", "OUTER_TYPE", "INNER_TYPE", "")
        val exception = shouldThrow<BadDecryptedDataException> {
            validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        }
        exception.message shouldBe "Exception in processing the decrypted record id '00003' in db 'db' in collection 'collection' with the reason 'Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive'"
        verify(counter, times(1)).inc()
        verifyNoMoreInteractions(counter)
    }

    @Test
    fun shouldRemoveArchiveDateTimeIfRemovedDateTimePresent() {
        val decryptedDbObject = """{
            |    "_id": {
            |       "id": "12345"
            |    },
            |    "_archivedDateTime": "2021-10-10T03:35:51.145+0000",
            |    "_removedDateTime": "2021-10-12T10:06:01.280+0000",
            |    "_lastModifiedDateTime": "2021-10-02T14:02:16.653+0000"
            |}""".trimMargin()
        val encryptionBlock =
            EncryptionBlock("keyEncryptionKeyId", "initialisationVector", "encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"),
            encryptionBlock, "dbObject", 1000,"db", "collection", "OUTER_TYPE", "INNER_TYPE", "")
        val decrypted: DecryptedRecord? = validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        assertNotNull(decrypted)
        decrypted?.let { (dbObject, _) ->
            assertTrue(dbObject.has("_removedDateTime"))
            assertFalse(dbObject.has("_archivedDateTime"))
        }
    }

    @Test
    fun shouldNotRemoveArchiveDateTimeIfRemovedDateTimeNotPresent() {
        val decryptedDbObject = """{
            |    "_id": {
            |       "id": "12345"
            |    },
            |    "_archivedDateTime": "2021-10-10T03:35:51.145+0000",
            |    "_lastModifiedDateTime": "2021-10-02T14:02:16.653+0000"
            |}""".trimMargin()
        val encryptionBlock =
            EncryptionBlock("keyEncryptionKeyId", "initialisationVector", "encryptedEncryptionKey")
        val sourceRecord = SourceRecord(generateFourByteChecksum("00001"),
            encryptionBlock, "dbObject", 1000,"db", "collection", "OUTER_TYPE", "INNER_TYPE", "")
        val decrypted: DecryptedRecord? = validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        assertNotNull(decrypted)
        decrypted?.let { (dbObject, _) ->
            assertTrue(dbObject.has("_archivedDateTime"))
            assertFalse(dbObject.has("_removedDateTime"))
        }
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
        val jsonObject = parse(decryptedDbObject)
        val idJsonObject = validator.retrieveId(jsonObject)
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
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
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
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
        assertEquals(dateOne, actual)
    }

    @Test
    fun Should_Retrieve_RemovedDateTime_When_Present_And_LastModifiedDateTime_Is_Missing() {
        val dateOne = "2019-12-14T15:01:02.000+0000"

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_removedDateTime": "$dateOne"
                }"""
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
        assertEquals(dateOne, actual)
    }

    @Test
    fun Should_Retrieve_RemovedDateTime_When_Present_And_LastModifiedDateTime_Is_Blank() {
        val dateOne = "2019-12-14T15:01:02.000+0000"

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "",
                   "_removedDateTime": "$dateOne"
                }"""
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
        assertEquals(dateOne, actual)
    }

    @Test
    fun Should_Retrieve_RemovedDateTime_When_Present_And_LastModifiedDateTime_Is_Null() {
        val dateOne = "2019-12-14T15:01:02.000+0000"

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": null,
                   "_removedDateTime": "$dateOne"
                }"""
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
        assertEquals(dateOne, actual)
    }

    @Test
    fun Should_Retrieve_RemovedDateTime_When_It_And_CreatedDateTime_Are_Present_And_LastModifiedDateTime_Is_Missing() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val dateTwo = "2018-12-14T15:01:02.000+0000"

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_removedDateTime": "$dateOne",
                   "createdDateTime": "$dateTwo"
                }"""
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
        assertEquals(dateOne, actual)
    }

    @Test
    fun Should_Retrieve_RemovedDateTime_When_It_And_CreatedDateTime_Are_Present_As_Objects_And_LastModifiedDateTime_Is_Missing() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val dateTwo = "2018-12-14T15:01:02.000+0000"

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_removedDateTime": {"${"$"}date": "$dateOne"},
                   "createdDateTime": {"${"$"}date": "$dateTwo"}
                }"""
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
        assertEquals(dateOne, actual)
    }

    @Test
    fun Should_Retrieve_CreatedDateTime_When_Present_And_LastModifiedDateTime_Is_Missing() {
        val dateOne = "2019-12-14T15:01:02.000+0000"

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "createdDateTime": "$dateOne"
                }"""
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
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
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
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
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
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
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
        assertEquals(dateOne, actual)
    }

    @Test
    fun Should_Retrieve_Epoch_When_CreatedDateTime_And_LastModifiedDateTime_Are_Missing() {
        val epoch = "1980-01-01T00:00:00.000Z"

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"}
                }"""
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
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
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
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
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
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
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveLastModifiedDateTime(jsonObject)
        assertEquals(epoch, actual)
    }

    @Test
    fun Should_Retrieve_Value_String_When_Date_Element_Is_String() {
        val expected = "A Date"

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "dateTimeTestElement": "$expected"
                }"""
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveDateTimeElement("dateTimeTestElement", jsonObject)
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Retrieve_Value_String_When_Date_Element_Is_Valid_Object() {
        val expected = "A Date"

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "dateTimeTestElement": {"${"$"}date": "$expected"}
                }"""
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveDateTimeElement("dateTimeTestElement", jsonObject)
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Retrieve_Empty_String_When_Date_Element_Is_InValid_Object() {
        val expected = ""

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "dateTimeTestElement": {"date": "$expected"}
                }"""
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveDateTimeElement("dateTimeTestElement", jsonObject)
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Retrieve_Empty_String_When_Date_Element_Is_Null() {
        val expected = ""

        val decryptedDbObject = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "dateTimeTestElement": null
                }"""
        val jsonObject = parse(decryptedDbObject)
        val actual = validator.retrieveDateTimeElement("dateTimeTestElement", jsonObject)
        assertEquals(expected, actual)
    }

    @Test
    fun toleratesAbsenceOfId() {
        val encryptionBlock =
                EncryptionBlock("keyEncryptionKeyId",
                        "initialisationVector",
                        "encryptedEncryptionKey")
        val decryptedDbObject = """{
                   "_id1":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000"
                }"""
        val sourceRecord = SourceRecord("""1234{ "id": "12345" }""".toByteArray(), encryptionBlock,
                "dbObject", 1000, "db", "collection",
                "OUTER_TYPE", "INNER_TYPE", "")
        val decryptedRecord = validator.skipBadDecryptedRecords(sourceRecord, decryptedDbObject)
        decryptedRecord.shouldNotBeNull()
        decryptedRecord.manifestRecord shouldBe
                ManifestRecord("""{"${'$'}oid":"12345"}""", 1000L, "db", "collection",
                    "EXPORT","OUTER_TYPE", "INNER_TYPE", "12345")

        decryptedRecord.dbObject.toString() shouldMatchJson """{
            "_id1":{
                "test_key_a":"test_value_a",
                "test_key_b":"test_value_b"
            },
            "_lastModifiedDateTime":{"${'$'}date":"2018-12-14T15:01:02.000Z"}}                                                               
        """.trimIndent()
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

        val oldJsonObject = parse(oldJson)
        val expected = parse(newJson)
        val actual = validator.replaceElementValueWithKeyValuePair(oldJsonObject, "_lastModifiedDateTime", "${"$"}date", newDate)

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

        val oldJsonObject = parse(oldJson)
        val expected = parse(newJson)
        val actual = validator.replaceElementValueWithKeyValuePair(oldJsonObject, "_lastModifiedDateTime", "${"$"}date", newDate)

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

        val oldJsonObject = parse(oldJson)
        val expected = parse(newJson)
        val actual = validator.replaceElementValueWithKeyValuePair(oldJsonObject, "_lastModifiedDateTime", "${"$"}date", newDate)

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

        val oldJsonObject = parse(oldJson)
        val expected = parse(newJson)
        val actual = validator.replaceElementValueWithKeyValuePair(oldJsonObject, "_lastModifiedDateTime", "${"$"}date", newDate)

        assertEquals(expected, actual)
    }

    @Test
    fun Should_Wrap_All_Dates() {
        val dateOne = "2019-12-14T15:01:02.000Z"
        val dateTwo = "2018-12-14T15:01:02.000Z" 
        val dateThree = "2017-12-14T15:01:02.000Z" 
        val dateFour = "2016-12-14T15:01:02.000Z" 
        val dateKey = "\$date"
        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "$dateOne",
                   "createdDateTime": "$dateTwo",
                   "_removedDateTime": "$dateThree",
                   "_archivedDateTime": "$dateFour"
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "createdDateTime": {"$dateKey": "$dateTwo"},
                    "_removedDateTime": {"$dateKey": "$dateThree"},
                    "_archivedDateTime": {"$dateKey": "$dateFour"},
                    "_lastModifiedDateTime": {"$dateKey": "$dateOne"}
                }"""

        val oldJsonObject = parse(oldJson)
        val expected = parse(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject)

        assertEquals(expected, actual)
        assertEquals(dateOne, lastModifiedDate)
    }

    @Test
    fun Should_Format_All_Unwrapped_Dates() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val dateTwo = "2018-12-14T15:01:02.000+0000" 
        val dateThree = "2017-12-14T15:01:02.000+0000" 
        val dateFour = "2016-12-14T15:01:02.000+0000" 
        val dateKey = "\$date"
        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "$dateOne",
                   "createdDateTime": "$dateTwo",
                   "_removedDateTime": "$dateThree",
                   "_archivedDateTime": "$dateFour"
                }"""

        val formattedDateOne = "2019-12-14T15:01:02.000Z"
        val formattedDateTwo = "2018-12-14T15:01:02.000Z" 
        val formattedDateThree = "2017-12-14T15:01:02.000Z"
        val formattedDateFour = "2016-12-14T15:01:02.000Z"

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "createdDateTime": {"$dateKey": "$formattedDateTwo"},
                    "_removedDateTime": {"$dateKey": "$formattedDateThree"},
                    "_archivedDateTime": {"$dateKey": "$formattedDateFour"},
                    "_lastModifiedDateTime": {"$dateKey": "$formattedDateOne"}
                }"""

        val oldJsonObject = parse(oldJson)
        val expected = parse(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject)

        assertEquals(expected, actual)
        assertEquals(dateOne, lastModifiedDate)
    }

    @Test
    fun Should_Keep_Wrapped_Dates_Within_Wrapper() {
        val dateOne = "2019-12-14T15:01:02.000Z"
        val dateTwo = "2018-12-14T15:01:02.000Z" 
        val dateThree = "2017-12-14T15:01:02.000Z" 
        val dateFour = "2016-12-14T15:01:02.000Z"
        val dateKey = "\$date"

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {"${"$"}date": "$dateOne"},
                   "createdDateTime": {"${"$"}date": "$dateTwo"},
                   "_removedDateTime": {"${"$"}date": "$dateThree"},
                   "_archivedDateTime": {"${"$"}date": "$dateFour"}
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "createdDateTime": {"$dateKey": "$dateTwo"},
                    "_removedDateTime": {"$dateKey": "$dateThree"},
                    "_archivedDateTime": {"$dateKey": "$dateFour"},
                    "_lastModifiedDateTime": {"$dateKey": "$dateOne"}
                }"""

        val oldJsonObject = parse(oldJson)
        val expected = parse(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject)

        assertEquals(expected, actual)
        assertEquals(dateOne, lastModifiedDate)
    }

    @Test
    fun Should_Format_All_Wrapped_Dates() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val dateTwo = "2018-12-14T15:01:02.000+0000" 
        val dateThree = "2017-12-14T15:01:02.000+0000" 
        val dateFour = "2016-12-14T15:01:02.000+0000" 

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": {"${"$"}date": "$dateOne"},
                   "createdDateTime": {"${"$"}date": "$dateTwo"},
                   "_removedDateTime": {"${"$"}date": "$dateThree"},
                   "_archivedDateTime": {"${"$"}date": "$dateFour"}
                }"""

        val formattedDateOne = "2019-12-14T15:01:02.000Z"
        val formattedDateTwo = "2018-12-14T15:01:02.000Z" 
        val formattedDateThree = "2017-12-14T15:01:02.000Z" 
        val formattedDateFour = "2016-12-14T15:01:02.000Z"

        val dateKey = "\$date"
        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "createdDateTime": {"$dateKey": "$formattedDateTwo"},
                    "_removedDateTime": {"$dateKey": "$formattedDateThree"},
                    "_archivedDateTime": {"$dateKey": "$formattedDateFour"},
                    "_lastModifiedDateTime": {"$dateKey": "$formattedDateOne"}
                }"""

        val oldJsonObject = parse(oldJson)
        val expected = parse(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject)

        assertEquals(expected, actual)
        assertEquals(dateOne, lastModifiedDate)
    }

    @Test
    fun Should_Allow_For_Missing_Created_Removed_And_Archived_Dates() {
        val dateOne = "2019-12-14T15:01:02.000Z"

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "$dateOne"
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$dateOne"}
                }"""

        val oldJsonObject = parse(oldJson)
        val expected = parse(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject)

        assertEquals(expected, actual)
        assertEquals(dateOne, lastModifiedDate)
    }

    @Test
    fun Should_Allow_For_Empty_Created_Removed_And_Archived_Dates() {
        val dateOne = "2019-12-14T15:01:02.000Z"

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "$dateOne",
                   "createdDateTime": "",
                   "_removedDateTime": "",
                   "_archivedDateTime": ""
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$dateOne"},
                    "createdDateTime": "",
                    "_removedDateTime": "",
                    "_archivedDateTime": ""
                }"""

        val oldJsonObject = parse(oldJson)
        val expected = parse(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject)

        assertEquals(expected, actual)
        assertEquals(dateOne, lastModifiedDate)
    }

    @Test
    fun Should_Allow_For_Null_Created_Removed_And_Archived_Dates() {
        val dateOne = "2019-12-14T15:01:02.000Z"

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "$dateOne",
                   "createdDateTime": null,
                   "_removedDateTime": null,
                   "_archivedDateTime": null
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$dateOne"},
                    "createdDateTime": null,
                    "_removedDateTime": null,
                    "_archivedDateTime": null
                }"""

        val oldJsonObject = parse(oldJson)
        val expected = parse(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject)

        assertEquals(expected, actual)
        assertEquals(dateOne, lastModifiedDate)
    }

    @Test
    fun Should_Create_Last_Modified_If_Missing_Dates_If_Asked() {
        val epoch = "1980-01-01T00:00:00.000Z"

        val oldJson = """{
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"}
                }"""

        val newJson = """{
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": {"${"$"}date": "$epoch"}
                }"""

        val oldJsonObject = parse(oldJson)
        val expected = parse(newJson)
        val (actual, lastModifiedDate) = validator.wrapDates(oldJsonObject)

        assertEquals(expected, actual)
        assertEquals(epoch, lastModifiedDate)
    }

    @Test
    fun Should_Parse_Valid_Incoming_Date_Format() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ")
        val expected = df.parse(dateOne)
        val actual = validator.getValidParsedDateTime(dateOne)

        assertEquals(expected, actual)
    }

    @Test
    fun Should_Parse_Valid_Outgoing_Date_Format() {
        val dateOne = "2019-12-14T15:01:02.000Z"
        val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        val expected = df.parse(dateOne)
        val actual = validator.getValidParsedDateTime(dateOne)
        
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Throw_Error_With_Invalid_Date_Format() {
        val exception = shouldThrow<ParseException> {
            validator.getValidParsedDateTime("2019-12-14T15:01:02")
        }

        exception.message shouldBe "Unparseable date found: '2019-12-14T15:01:02', did not match any supported date formats"
    }

    @Test
    fun Should_Return_Created_Timestamp_When_Valid_And_Snapshot_Type_Is_Full() {
        val createdDateTime = "2019-12-14T15:01:02.000Z"
        val lastModifiedDateTime = "2018-12-14T15:01:02.000Z"
        val snapshotType = "full"
        val expected = 1576335662000L
        val actual = validator.timestampAsLong(createdDateTime, lastModifiedDateTime, snapshotType)
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Return_Last_Modified_Timestamp_When_Valid_And_Snapshot_Type_Is_Not_Full() {
        val createdDateTime = "2018-12-14T15:01:02.000Z"
        val lastModifiedDateTime = "2019-12-14T15:01:02.000Z"
        val snapshotType = "not_full"
        val expected = 1576335662000L
        val actual = validator.timestampAsLong(createdDateTime, lastModifiedDateTime, snapshotType)
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Return_Last_Modified_Timestamp_When_Created_Not_Valid_And_Snapshot_Type_Is_Full() {
        val createdDateTime = "32201-9-12-14T15:01:02.000Z"
        val lastModifiedDateTime = "2019-12-14T15:01:02.000Z"
        val snapshotType = "full"
        val expected = 1576335662000L
        val actual = validator.timestampAsLong(createdDateTime, lastModifiedDateTime, snapshotType)
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Return_Created_Timestamp_When_Last_Modified_Not_Valid_And_Snapshot_Type_Is_Not_Full() {
        val createdDateTime = "2019-12-14T15:01:02.000Z"
        val lastModifiedDateTime = "20d19-12-14T15:01ff02.000Z"
        val snapshotType = "not_full"
        val expected = 1576335662000L
        val actual = validator.timestampAsLong(createdDateTime, lastModifiedDateTime, snapshotType)
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Change_Incoming_UTC_Format_Date_To_Outgoing_Format() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val expected = "2019-12-14T15:01:02.000Z"
        val actual = validator.formatDateTimeToValidOutgoingFormat(dateOne)
        
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Not_Change_Date_Already_In_Outgoing_Format() {
        val dateOne = "2019-12-14T15:01:02.000Z"
        val expected = "2019-12-14T15:01:02.000Z"
        val actual = validator.formatDateTimeToValidOutgoingFormat(dateOne)
        
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Change_Positive_Offset_Date_To_UTC() {
        val dateOne = "2019-12-14T15:01:02.000+0100"
        val expected = "2019-12-14T14:01:02.000Z"
        val actual = validator.formatDateTimeToValidOutgoingFormat(dateOne)
        
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Change_Negative_Offset_Date_To_UTC() {
        val dateOne = "2019-12-14T15:01:02.000-0100"
        val expected = "2019-12-14T16:01:02.000Z"
        val actual = validator.formatDateTimeToValidOutgoingFormat(dateOne)
        
        assertEquals(expected, actual)
    }

    private fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()
        checksum.update(bytes, 0, bytes.size)
        val checksumBytes = ByteBuffer.allocate(4).putInt(checksum.value.toInt()).array()
        return checksumBytes.plus(bytes)
    }

    fun parse(string: String): JsonObject = gson.fromJson(string, JsonObject::class.java)

    @SpyBean
    private lateinit var validator: Validator

    @MockBean
    private lateinit var counter: Counter
    private val gson = GsonBuilder().serializeNulls().create()
}

