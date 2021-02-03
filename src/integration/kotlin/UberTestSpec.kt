
import app.services.CipherService
import app.services.KeyService
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model.Message
import com.google.gson.Gson
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import io.kotest.assertions.json.shouldMatchJson
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
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
            s3BucketKeys("exports") shouldContainExactly expectedExports()
        }

        "Should use a single datakey" {
            s3BucketObjects("exports").mapNotNull {
                with(it.objectMetadata.userMetadata) {
                    get("datakeyencryptionkeyid")?.let { masterKeyId ->
                        get("ciphertext")?.let { encryptedKey ->
                            keyService.decryptKey(masterKeyId, encryptedKey)
                        }
                    }
                }
            }.toSet() shouldHaveSize 1
        }

        "Writes the manifests" {
            s3BucketKeys("manifests") shouldContainExactly expectedManifests()
        }

        "Writes the correct manifest entries" {
            val entries = s3BucketObjects("manifests")
                    .map(S3Object::getObjectContent)
                    .map { it.copyToByteArrayOutputStream() }.lines().toList()

            entries shouldHaveSize 10_000

            entries.map {it.split(Regex("""\|""")) }.map {it[1]}.forEach {
                it shouldBe "1000"
            }

            val (modified, unmodified) = entries.partition {
                val data = it.split(Regex("""\|"""))
                data[0].contains("\$oid")
            }

            modified shouldHaveSize 5_000
            unmodified shouldHaveSize 5_000
        }

        "Writes the correct records" {
            val ids = s3BucketObjects("exports")
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
                            Triple(inputStream.copyToByteArray(), it.second, it.third)
                        }
                    }
                    .map { decrypting(it.second, it.third, it.first) }
                    .map { it.copyToByteArrayOutputStream() }
                    .lines()
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
            val correlationIdAttributeValue = AttributeValue().apply { s = "s3-export" }
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
            filesExported?.n shouldBe "20"
            filesSent?.n shouldBe "0"
        }

        "Correct messages sent" {
            val received = queueMessages(snapshotSenderQueueUrl)
                .map(Message::getBody)
                .map {Gson().fromJson(it, JsonObject::class.java)}
            received shouldHaveSize 21
            val pathValues = received.mapNotNull { it.remove("s3_full_folder") }.map(JsonElement::getAsString).sorted()

            val expected = expectedExports()

            pathValues shouldContainExactly expected

            val (noFilesExportedMessages, filesExportedMessages) = received.map(JsonObject::toString).partition {
                it.contains("files_exported")
            }

            filesExportedMessages shouldHaveSize 20
            filesExportedMessages.forEach {
                it shouldMatchJson """{
                        "shutdown_flag":"false",
                        "correlation_id":"s3-export",
                        "topic_name":"db.database.collection",
                        "export_date":"2020-07-06",
                        "reprocess_files":"true",
                        "snapshot_type":"full"
                    }"""
            }

            noFilesExportedMessages shouldHaveSize 1
            noFilesExportedMessages[0] shouldMatchJson """{
                            "shutdown_flag":"false",
                            "correlation_id":"empty-export",
                            "topic_name":"db.database.empty",
                            "export_date":"2020-07-06",
                            "reprocess_files":"true",
                            "snapshot_type":"full",
                            "files_exported":0
                        }"""

        }

        "It should send the successful completion message" {
            validateQueueMessage(adgQueueUrl, """{
                    "correlation_id": "s3-export",
                    "s3_prefix": "output",
                    "trigger_adg": true
                }""")
        }

        "It should send the monitoring message" {
            validateQueueMessage(monitoringQueueUrl, """{
                    "severity": "Critical",
                    "notification_type": "Information",
                    "slack_username": "Crown Export Poller",
                    "title_text": "Full - Export finished - success",
                    "custom_elements":[
                        {
                            "key":"Export date",
                            "value":"2020-07-06"
                        },
                        {
                            "key":"Correlation Id",
                            "value":"s3-export"
                        }
                    ]
                }""")
        }

        "dynamoDB should have no files record" {
            val tableName = "UCExportToCrownStatus"
            val correlationIdAttributeValue = AttributeValue().apply {s = "empty-export"}
            val collectionNameAttributeValue = AttributeValue().apply {s = "db.database.empty"}
            val primaryKey = primaryKeyMap(correlationIdAttributeValue, collectionNameAttributeValue)
            val getItemRequest = getItemRequest(tableName, primaryKey)
            val result = amazonDynamoDB.getItem(getItemRequest)
            val item = result.item
            item.shouldNotBeNull()
            item["CollectionStatus"]?.s shouldBe "Exported"
            item["FilesExported"]?.n shouldBe "0"
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
            val correlationIdAttributeValue = AttributeValue().apply {s = "table-unavailable"}
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

    private fun validateQueueMessage(queueUrl: String, expectedMessage: String) {
        val received = queueMessages(queueUrl)
            .map(Message::getBody)
            .map {Gson().fromJson(it, JsonObject::class.java)}
            .mapNotNull { it["Message"] }

        received shouldHaveSize 1
        received.first().asString shouldMatchJson expectedMessage
    }

    companion object {
        private val applicationContext by lazy { AnnotationConfigApplicationContext(TestConfiguration::class.java) }
        private val amazonS3 by lazy { applicationContext.getBean(AmazonS3::class.java) }
        private val amazonDynamoDB by lazy { applicationContext.getBean(AmazonDynamoDB::class.java) }
        private val amazonSqs by lazy { applicationContext.getBean(AmazonSQS::class.java) }
        private val keyService by lazy { applicationContext.getBean(KeyService::class.java) }
        private val cipherService by lazy { applicationContext.getBean(CipherService::class.java)}

        private const val snapshotSenderQueueUrl = "http://aws:4566/000000000000/integration-queue"
        private const val adgQueueUrl = "http://aws:4566/000000000000/trigger-adg-subscriber"
        private const val monitoringQueueUrl = "http://aws:4566/000000000000/monitoring-subscriber"

        fun InputStream.copyToByteArray(): ByteArray = this.copyToByteArrayOutputStream().toByteArray()

        fun InputStream.copyToByteArrayOutputStream() =
            use { inputStream -> ByteArrayOutputStream().also { inputStream.copyTo(it) } }

        fun List<ByteArrayOutputStream>.lines() =
                this.map(ByteArrayOutputStream::toByteArray)
                        .map(ByteArray::decodeToString)
                        .toList()
                        .flatMap { it.split("\n") }
                        .filter(String::isNotBlank).asSequence()


        private fun s3BucketObjects(bucket: String) = s3BucketKeys(bucket).map { amazonS3.getObject(bucket, it) }
        private fun s3BucketKeys(bucket: String)= s3Objects(bucket).objectSummaries.map(S3ObjectSummary::getKey)
        private fun s3Objects(bucket: String)= amazonS3.listObjects(bucket)

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

        private tailrec fun queueMessages(queueUrl: String, accumulated: List<Message> = listOf()): List<Message> {
            val messages = amazonSqs.receiveMessage(queueUrl).messages

            if (messages == null || messages.isEmpty()) {
                return accumulated
            }
            messages.forEach{ deleteMessage(queueUrl, it) }
            return queueMessages(queueUrl, accumulated + messages)
        }

        private fun deleteMessage(queueUrl: String, it: Message) = amazonSqs.deleteMessage(queueUrl, it.receiptHandle)
        
        private fun expectedExports(): List<String> =
                listOf("output/db.database.collection-000-040-000001.txt.bz2.enc",
                        "output/db.database.collection-000-040-000002.txt.bz2.enc",
                        "output/db.database.collection-000-040-000003.txt.bz2.enc",
                        "output/db.database.collection-008-000-000001.txt.bz2.enc",
                        "output/db.database.collection-040-080-000001.txt.bz2.enc",
                        "output/db.database.collection-040-080-000002.txt.bz2.enc",
                        "output/db.database.collection-040-080-000003.txt.bz2.enc",
                        "output/db.database.collection-048-008-000001.txt.bz2.enc",
                        "output/db.database.collection-048-008-000002.txt.bz2.enc",
                        "output/db.database.collection-048-008-000003.txt.bz2.enc",
                        "output/db.database.collection-080-120-000001.txt.bz2.enc",
                        "output/db.database.collection-080-120-000002.txt.bz2.enc",
                        "output/db.database.collection-080-120-000003.txt.bz2.enc",
                        "output/db.database.collection-088-048-000001.txt.bz2.enc",
                        "output/db.database.collection-088-048-000002.txt.bz2.enc",
                        "output/db.database.collection-088-048-000003.txt.bz2.enc",
                        "output/db.database.collection-120-128-000001.txt.bz2.enc",
                        "output/db.database.collection-128-088-000001.txt.bz2.enc",
                        "output/db.database.collection-128-088-000002.txt.bz2.enc",
                        "output/db.database.collection-128-088-000003.txt.bz2.enc")

        private fun expectedManifests(): List<String> =
                listOf("output/db.database.collection-000-040-000000.csv",
                        "output/db.database.collection-000-040-000001.csv",
                        "output/db.database.collection-000-040-000002.csv",
                        "output/db.database.collection-008-000-000000.csv",
                        "output/db.database.collection-040-080-000000.csv",
                        "output/db.database.collection-040-080-000001.csv",
                        "output/db.database.collection-040-080-000002.csv",
                        "output/db.database.collection-048-008-000000.csv",
                        "output/db.database.collection-048-008-000001.csv",
                        "output/db.database.collection-048-008-000002.csv",
                        "output/db.database.collection-080-120-000000.csv",
                        "output/db.database.collection-080-120-000001.csv",
                        "output/db.database.collection-080-120-000002.csv",
                        "output/db.database.collection-088-048-000000.csv",
                        "output/db.database.collection-088-048-000001.csv",
                        "output/db.database.collection-088-048-000002.csv",
                        "output/db.database.collection-120-128-000000.csv",
                        "output/db.database.collection-128-088-000000.csv",
                        "output/db.database.collection-128-088-000001.csv",
                        "output/db.database.collection-128-088-000002.csv")
    }
}
