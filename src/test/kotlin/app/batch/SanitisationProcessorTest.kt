package app.batch

import app.domain.DecryptedRecord
import app.domain.ManifestRecord
import com.google.gson.Gson
import com.google.gson.JsonObject
import org.assertj.core.api.Assertions.assertThat
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [SanitisationProcessor::class])
class SanitisationProcessorTest {

    @Test
    fun shouldRemoveDesiredCharsInCollections() {
        val jsonWithRemovableChars = "{ \"fieldA\": \"a$\u0000\", \"_archivedDateTime\": \"b\", \"_archived\": \"c\" }"
        val input = DecryptedRecord(Gson().fromJson(jsonWithRemovableChars, JsonObject::class.java), ManifestRecord("", 0, "db", "collection", "EXPORT", "OUTER_TYPE", "INNER_TYPE", "OID"))
        val expectedOutput = """{"fieldA":"ad_","_removedDateTime":"b","_removed":"c"}"""

        val actualOutput = sanitisationProcessor.process(input)?.dbObjectAsString
        assertThat(actualOutput).isEqualTo(expectedOutput)
    }

    @Test
    fun shouldNotRemoveMultiEscapedNewlines() {
        val data = """{"message":{"db":"penalties-and-deductions","collection":"sanction"},"data":{"carriage":"\\r","newline":"\\n","superEscaped":"\\\r\\\n"}}"""
        val input = DecryptedRecord(Gson().fromJson(data, JsonObject::class.java), ManifestRecord("", 0, "db", "collection", "EXPORT", "OUTER_TYPE", "INNER_TYPE", "OID"))
        val actualOutput = sanitisationProcessor.process(input)?.dbObjectAsString
        assertThat(actualOutput).isEqualTo(data)
    }

    @Test
    fun shouldRemoveDesiredCharsFromSpecificCollections() {
        var input = DecryptedRecord(getInputDBObject(), ManifestRecord("", 0, "penalties-and-deductions", "sanction", "EXPORT", "OUTER_TYPE", "INNER_TYPE", "OID"))
        var expected = getOutputDBObject()
        var actual = sanitisationProcessor.process(input)?.dbObjectAsString

        assertTrue(expected == actual)

        input = DecryptedRecord(getInputDBObject(), ManifestRecord("", 0, "penalties-and-deductions", "sanction", "EXPORT", "OUTER_TYPE", "INNER_TYPE", "OID"))
        expected = getOutputDBObject()
        actual = sanitisationProcessor.process(input)?.dbObjectAsString
        assertEquals(expected, actual)

        input = DecryptedRecord(getInputDBObject(), ManifestRecord("", 0, "penalties-and-deductions", "sanction", "EXPORT", "OUTER_TYPE", "INNER_TYPE", "OID"))
        expected = getOutputDBObject()
        actual = sanitisationProcessor.process(input)?.dbObjectAsString
        assertEquals(expected, actual)
    }

    @Test
    fun shouldNotRemoveCharsFromOtherCollections() {
        val input = DecryptedRecord(getInputDBObject(), ManifestRecord("", 0, "db", "collection", "EXPORT", "OUTER_TYPE", "INNER_TYPE", "OID"))
        val expected = getOutputDBObject()
        val actual = sanitisationProcessor.process(input)
        assertNotEquals(expected, actual)
    }

    fun getInputDBObject(): JsonObject {
        val data = """{
              "_id": {
                "declarationId": "47a4fad9-49af-4cb2-91b0-0056e2ac0eef\r"
              },
              "type": "addressDeclaration\n",
              "contractId": "aa16e682-fbd6-4fe3-880b-118ac09f992a\r\n",
              "addressNumber": {
                "type": "AddressLine",
                "cryptoId": "bd88a5f9-ab47-4ae0-80bf-e53908457b60"
              },
              "addressLine2": null,
              "townCity": {
                "type": "AddressLine",
                "cryptoId": "9ca3c63c-cbfc-452a-88fd-bb97f856fe60"
              },
              "postcode": "SM5 2LE",
              "processId": "3b313df5-96bc-40ff-8128-07d496379664",
              "effectiveDate": {
                "type": "SPECIFIC_EFFECTIVE_DATE",
                "date": 20150320,
                "knownDate": 20150320
              },
              "paymentEffectiveDate": {
                "type": "SPECIFIC_EFFECTIVE_DATE\r\n",
                "date": 20150320,
                "knownDate": 20150320
              },
              "createdDateTime": {
                "${'$'}date": "2015-03-20T12:23:25.183Z"
              },
              "_version": 2,
              "_lastModifiedDateTime": {
                "${'$'}date": "2016-06-23T05:12:29.624Z"
              }
        }"""
        return Gson().fromJson(data, JsonObject::class.java)
    }

    fun getOutputDBObject(): String {
        val data = """
              {
              "_id": {
               "declarationId": "47a4fad9-49af-4cb2-91b0-0056e2ac0eef"
              },
              "type": "addressDeclaration",
              "contractId": "aa16e682-fbd6-4fe3-880b-118ac09f992a",
              "addressNumber": {
               "type": "AddressLine",
                "cryptoId": "bd88a5f9-ab47-4ae0-80bf-e53908457b60"
              },
              "addressLine2": null,
              "townCity": {
               "type": "AddressLine",
               "cryptoId": "9ca3c63c-cbfc-452a-88fd-bb97f856fe60"
              },
              "postcode": "SM5 2LE",
              "processId": "3b313df5-96bc-40ff-8128-07d496379664",
              "effectiveDate": {
               "type": "SPECIFIC_EFFECTIVE_DATE",
               "date": 20150320,
               "knownDate": 20150320
              },
              "paymentEffectiveDate": {
               "type": "SPECIFIC_EFFECTIVE_DATE",
               "date": 20150320,
               "knownDate": 20150320
              },
              "createdDateTime": {
               "d_date": "2015-03-20T12:23:25.183Z"
              },
              "_version": 2,
              "_lastModifiedDateTime": {
               "d_date": "2016-06-23T05:12:29.624Z"
              }
              }"""
        return Gson().fromJson(data, JsonObject::class.java).toString()
    }

    @Autowired
    private lateinit var sanitisationProcessor: SanitisationProcessor
}
