import app.services.CipherService
import app.services.KeyService
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model.Message
import com.google.gson.Gson
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.util.*
import javax.crypto.CipherInputStream
import javax.crypto.spec.SecretKeySpec

class UberTestSpec: StringSpec() {
    init {
        "Writes the correct objects" {
            val actual = amazonS3.listObjects("demobucket").objectSummaries.map(S3ObjectSummary::getKey)
            val expected = listOf(
                    "test-exporter/db.database.collection-000-040-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-008-000-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-040-080-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-048-008-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-080-120-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-088-048-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-120-128-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-128-088-000001.txt.bz2.enc")
            actual shouldContainExactly expected
        }

        "Writes the manifests" {
            val actual = amazonS3.listObjects("manifestbucket").objectSummaries.map(S3ObjectSummary::getKey)

        }

        "Writes the correct records" {
            val ids = amazonS3.listObjects("demobucket").objectSummaries
                    .asSequence()
                    .map(S3ObjectSummary::getKey)
                    .map { amazonS3.getObject("demobucket", it) }
                    .mapNotNull {
                        with(it.objectMetadata.userMetadata) {
                            get("datakeyencryptionkeyid")?.let { masterKeyId ->
                                get("ciphertext")?.let { encryptedKey ->
                                    get("iv")?.let { iv ->
                                        Triple(it.objectContent, keyService.decryptKey(masterKeyId, encryptedKey), iv)
                                    }
                                }
                            }
                        }
                    }
                    .map {
                        it.first.use { inputStream ->
                            val outputStream = ByteArrayOutputStream()
                            inputStream.copyTo(outputStream)
                            Triple(outputStream.toByteArray(), it.second, it.third)
                        }
                    }
                    .map { decrypting(it.second, it.third, it.first) }
                    .map {
                        it.use { inputStream ->
                            ByteArrayOutputStream().also { outputStream -> inputStream.copyTo(outputStream) }
                        }
                    }
                    .map(ByteArrayOutputStream::toByteArray)
                    .map(ByteArray::decodeToString)
                    .toList()
                    .flatMap { it.split("\n") }
                    .asSequence()
                    .filter(String::isNotBlank)
                    .map { Gson().fromJson(it, JsonObject::class.java) }
                    .map { it["_id"].asJsonObject }
                    .map { if (it["record_id"] != null) it["record_id"] else it["d_oid"] }
                    .map(JsonElement::getAsJsonPrimitive)
                    .map(JsonPrimitive::getAsString).sorted()
                    .toList()

            ids.size shouldBe 10_000
            ids.forEachIndexed { index, id -> id shouldBe String.format("%05d", index) }
        }

        "Export status updated correctly" {
            val correlationIdAttributeValue = AttributeValue().apply { s = "integration_test_correlation_id" }
            val collectionNameAttributeValue = AttributeValue().apply { s = "db.database.collection" }
            val primaryKey = mapOf("CorrelationId" to correlationIdAttributeValue,
                    "CollectionName" to collectionNameAttributeValue)

            val getItemRequest = GetItemRequest().apply {
                tableName = "UCExportToCrownStatus"
                key = primaryKey
            }
            val result = amazonDynamoDB.getItem(getItemRequest)
            val item = result.item
            val status = item["CollectionStatus"]
            val filesExported = item["FilesExported"]
            val filesSent = item["FilesSent"]
            status?.s shouldBe "Exported"
            filesExported?.n shouldBe "8"
            filesSent?.n shouldBe "0"
        }

        "Correct messages sent" {
            val received = allMessages()
                .map(Message::getBody)
                .map {Gson().fromJson(it, JsonObject::class.java)}
            received shouldHaveSize 8
            val pathValues = received.map { it.remove("s3_full_folder") }.map(JsonElement::getAsString).sorted()
            val expected = listOf(
                    "test-exporter/db.database.collection-000-040-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-008-000-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-040-080-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-048-008-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-080-120-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-088-048-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-120-128-000001.txt.bz2.enc",
                    "test-exporter/db.database.collection-128-088-000001.txt.bz2.enc")

            pathValues shouldContainExactly expected

            received.forEach {
                it.toString() shouldBe """{
                    |"shutdown_flag":"false",
                    |"correlation_id":"integration_test_correlation_id",
                    |"topic_name":"db.database.collection",
                    |"export_date":"2020-07-06",
                    |"reprocess_files":"true",
                    |"snapshot_type":"full"
                    |}""".trimMargin().replace(Regex("""\s"""), "")
            }
        }

        "dynamoDB should have blocked topic record" {
            val tableName = "UCExportToCrownStatus"
            val correlationIdAttributeValue = AttributeValue().apply {s = "blocked_topic"}
            val collectionNameAttributeValue = AttributeValue().apply {s = "db.blocked.topic"}
            val primaryKey = primaryKeyMap(correlationIdAttributeValue, collectionNameAttributeValue)
            val getItemRequest = getItemRequest(tableName, primaryKey)
            val result = amazonDynamoDB.getItem(getItemRequest)
            val item = result.item
            item shouldNotBe null
            val status = item["CollectionStatus"]?.s
            val expectedCollectionStatus = "Blocked_Topic"
            status shouldBe expectedCollectionStatus
        }

        "dynamoDB has table unavailable record" {
            val tableName = "UCExportToCrownStatus"
            val correlationIdAttributeValue = AttributeValue().apply {s = "table_unavailable"}
            val collectionNameAttributeValue = AttributeValue().apply {s = "does.not.exist"}
            val primaryKey = primaryKeyMap(correlationIdAttributeValue, collectionNameAttributeValue)
            val getItemRequest = getItemRequest(tableName, primaryKey)
            val result = amazonDynamoDB.getItem(getItemRequest)
            val item = result.item
            val status = item["CollectionStatus"]?.s
            val expectedCollectionStatus = "Table_Unavailable"
            status shouldBe expectedCollectionStatus
        }
    }


    companion object {
        private val applicationContext by lazy { AnnotationConfigApplicationContext(TestConfiguration::class.java) }
        private val amazonS3 by lazy { applicationContext.getBean(AmazonS3::class.java) }
        private val amazonDynamoDB by lazy { applicationContext.getBean(AmazonDynamoDB::class.java) }
        private val amazonSqs by lazy { applicationContext.getBean(AmazonSQS::class.java) }
        private val keyService by lazy { applicationContext.getBean(KeyService::class.java) }
        private val cipherService by lazy { applicationContext.getBean(CipherService::class.java)}
        private const val sqsQueueUrl = "http://aws:4566/000000000000/integration-queue"

        private fun primaryKeyMap(correlationIdAttributeValue: AttributeValue, collectionNameAttributeValue: AttributeValue) =
                mapOf("CorrelationId" to correlationIdAttributeValue, "CollectionName" to collectionNameAttributeValue)

        private fun decrypting(key: String, initializationVector: String, encrypted: ByteArray): InputStream =
            SecretKeySpec(Base64.getDecoder().decode(key), "AES").let { keySpec ->
                cipherService.decryptingCipher(keySpec, Base64.getDecoder().decode(initializationVector)).let { cipher ->
                    CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.BZIP2,
                            CipherInputStream(ByteArrayInputStream(encrypted), cipher))
                }
            }

        fun getItemRequest(table: String, primaryKey: Map<String, AttributeValue>) = GetItemRequest().apply {
            tableName = table
            key = primaryKey
        }

        private fun allMessages(accumulated: List<Message> = listOf()): List<Message> {
            val messages = amazonSqs.receiveMessage(sqsQueueUrl).messages

            if (messages == null || messages.isEmpty()) {
                return accumulated
            }
            messages.forEach(::deleteMessage)
            return allMessages(accumulated + messages)
        }

        private fun deleteMessage(it: Message) = amazonSqs.deleteMessage(sqsQueueUrl, it.receiptHandle)
    }
}
