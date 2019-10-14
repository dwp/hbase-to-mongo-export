package app.batch.processor

import com.google.gson.Gson
import com.google.gson.JsonObject
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
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
class SanitisationProcessorTest {

    @Test
    fun testSanitisationProcessor_RemovesDesiredCharsInCollections() {
        val jsonWithRemovableChars =  "{ \"fieldA\": \"a$\\u0000\", \"_archivedDateTime\": \"b\", \"_archived\": \"c\" }"
        val input = Gson().fromJson(jsonWithRemovableChars, JsonObject::class.java)
        val expectedOutput =         """{"fieldA":"ad_","_removedDateTime":"b","_removed":"c"}"""

        val actualOutput = sanitisationProcessor.process(input)
        assertThat(actualOutput).isEqualTo(expectedOutput)
    }

    @Test
    fun testSanitisationProcessor_WillNotRemoveMultiEscapedNewlines() {
        val data = """{"message":{"db":"penalties-and-deductions","collection":"sanction"},"data":{"carriage":"\\r","newline":"\\n","superEscaped":"\\\r\\\n"}}"""

        val actualOutput = sanitisationProcessor.process(Gson().fromJson(data, JsonObject::class.java))
        assertThat(actualOutput).isEqualTo(data)
    }

    @Test
    fun testSanitisationProcessor_RemovesDesiredCharsFromSpecificCollections() {
        var input = collectionInputData("penalties-and-deductions", "sanction")
        var expected = collectionOutputData("penalties-and-deductions", "sanction")
        val actual = sanitisationProcessor.process(input)
        assertThat(actual).isEqualTo(expected)

        input = collectionInputData("core", "healthAndDisabilityDeclaration")
        expected = collectionOutputData("core", "healthAndDisabilityDeclaration")
        assertThat(sanitisationProcessor.process(input)).isEqualTo(expected)

        input = collectionInputData("accepted-data", "healthAndDisabilityCircumstances")
        expected = collectionOutputData("accepted-data", "healthAndDisabilityCircumstances")
        assertThat(sanitisationProcessor.process(input)).isEqualTo(expected)
    }

    @Test
    fun testSanitisationProcessor_DoesNotRemoveCharsFromOtherCollections() {
        val input = collectionInputData("some-other-db", "collectionName")
        val expected = collectionInputData("some-other-db", "collectionName").toString()
        val actual = sanitisationProcessor.process(input)
        assertThat(actual).isEqualTo(expected)
    }

    fun collectionInputData(db: String, collection: String): JsonObject {
        val data = """{
            |   "message": {
            |      "db": "$db",
            |      "collection": "$collection"
            |   },
            |   "chars": "\r\n"
            |}
        """.trimMargin()
        return Gson().fromJson(data, JsonObject::class.java)
    }

    fun collectionOutputData(db: String, collection: String): String {
        return """{
            |   "message": {
            |      "db": "$db",
            |      "collection": "$collection"
            |   },
            |   "charsToRemove": ""
            |}
        """.trimMargin().replace("\n", "").replace(" ", "")
    }

    @Autowired
    private lateinit var sanitisationProcessor: SanitisationProcessor
}