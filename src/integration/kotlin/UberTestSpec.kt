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
import io.kotest.matchers.collections.shouldBeIn
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.ktor.client.*
import io.ktor.client.features.json.*
import io.ktor.client.request.*
import io.mockk.InternalPlatformDsl.toStr
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.util.*
import javax.crypto.CipherInputStream
import javax.crypto.spec.SecretKeySpec


/**
 * Integration tests.
 *
 * Validate files written, the contents therein, messages sent, tables updated etc. etc.
 */
class UberTestSpec: StringSpec() {

    init {

        "It should have pushed metrics " {
            val response = client.get<JsonObject>("http://prometheus:9090/api/v1/targets/metadata")
            val metricNames = response["data"].asJsonArray
                .map(JsonElement::getAsJsonObject)
                .filter {
                    it["target"].asJsonObject["job"].asJsonPrimitive.asString == "pushgateway"
                }
                .map {
                    it["metric"].asJsonPrimitive.asString
                }
                .filterNot {
                    it.startsWith("go_") || it.startsWith("process_") ||
                            it.startsWith("pushgateway_") || it.startsWith("push_")
                }

            metricNames.sorted().forEach(::println)

            metricNames shouldContainAll listOf("htme_bytes_written",
                "htme_dks_decrypt_key_duration",
                "htme_dks_decrypt_retries",
                "htme_dks_decrypt_failures",
                "htme_dks_fetch_key_duration",
                "htme_dks_new_key_retries",
                "htme_dks_new_key_failures",
                "htme_failed_collections",
                "htme_records_failed_validation",
                "htme_records_written",
                "htme_retried_batch_puts",
                "htme_retried_manifest_puts",
                "htme_s3_batch_put_operation_duration",
                "htme_s3_manifest_put_operation_duration",
                "htme_successful_collections",
                "htme_successful_empty_collections",
                "htme_successful_non_empty_collections",
                "htme_running_applications",
                "htme_topic_duration",
                "htme_topics_started",
                "htme_topics_completed",
                "logback_appender_total",
                "spring_batch_chunk_write_seconds",
                "spring_batch_chunk_write_seconds_max",
                "spring_batch_item_process_seconds",
                "spring_batch_item_process_seconds_max",
                "spring_batch_item_read_seconds",
                "spring_batch_item_read_seconds_max",
                "spring_batch_job_active_seconds_active_count",
                "spring_batch_job_active_seconds_duration_sum",
                "spring_batch_job_active_seconds_max",
                "spring_batch_job_seconds",
                "spring_batch_job_seconds_max",
                "spring_batch_step_seconds",
                "spring_batch_step_seconds_max"
            )
        }

        "It should have pushed correct records read metrics " {
            // note that the extra 8 are the nulls that each reader returns as its final value,
            // so the expected value is no. of records read (10000) + no. of readers (8).
            validateMetric("""sum(spring_batch_item_read_seconds_count{topic="db.database.collection"})""", "10008")
        }

        "It should have pushed correct records processed metrics " {
            validateMetric("""sum(spring_batch_item_process_seconds_count{topic="db.database.collection"})""", "10000")
        }

        "It should have pushed correct records written metrics " {
            validateMetric("""sum(htme_records_written{topic="db.database.collection"})""", "10000")
        }

        "It should have pushed batch put written metrics " {
            validateMetric("""htme_s3_batch_put_operation_duration_count{topic="db.database.collection"}""", "20")
        }

        "It should have pushed correct manifest put metrics " {
            validateMetric("""htme_s3_manifest_put_operation_duration_count{topic="db.database.collection"}""", "20")
        }

        "It should have pushed correct successful collection metrics " {
            validateMetric("""htme_successful_collections{topic="db.database.collection"}""", "1")
        }

        "It should have pushed correct successful non empty collection metrics " {
            validateMetric("""htme_successful_non_empty_collections{topic="db.database.collection"}""", "1")
        }

        "It should have pushed correct topics started metrics " {
            validateMetric("""htme_topics_started{topic="db.database.collection"}""", "1")
        }

        "It should have pushed correct topics completed metrics " {
            validateMetric("""htme_topics_completed{topic="db.database.collection"}""", "1")
        }

        "Writes the correct objects" {
            s3BucketKeys("exports") shouldContainExactly expectedExports()
        }

        "Should use a single datakey for each collection" {
            s3BucketObjects("exports").mapNotNull {
                with(it.objectMetadata.userMetadata) {
                    get("datakeyencryptionkeyid")?.let { masterKeyId ->
                        get("ciphertext")?.let { encryptedKey ->
                            keyService.decryptKey(masterKeyId, encryptedKey)
                        }
                    }
                }
            }.toSet() shouldHaveSize 2
        }

        "Writes the manifests" {
            s3BucketKeys("manifests") shouldContainExactly expectedManifests()
        }

        "Writes the correct manifest entries" {
            val entries = s3BucketObjects("manifests")
                .map(S3Object::getObjectContent)
                .map { it.copyToByteArrayOutputStream() }.lines().toList()

            entries shouldHaveSize 20_000

            entries.map { it.split(Regex("""\|""")) }.map { it[1] }.forEach {
                it shouldBe "1000"
            }

            val (modified, unmodified) = entries.partition {
                val data = it.split(Regex("""\|"""))
                data[0].contains("\$oid")
            }

            modified shouldHaveSize 10_000
            unmodified shouldHaveSize 10_000
        }

        "Writes the correct records" {
            val (equalityRecords, regularRecords) = s3BucketObjects("exports")
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
                .partition { it.has("message") }

            validateIds(regularRecords) { x -> x["_id"].asJsonObject }
            validateIds(equalityRecords) { x -> x["message"].asJsonObject["_id"].asJsonObject }
        }

        "Export status updated correctly" {
            val correlationIdAttributeValue = AttributeValue().apply { s = S3_EXPORT_CORRELATION_ID }
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

        "Product status updated correctly" {
            val correlationIdAttributeValue = AttributeValue().apply { s = S3_EXPORT_CORRELATION_ID }
            val dataProductAttributeValue = AttributeValue().apply { s = "HTME" }
            val primaryKey = mapOf("Correlation_Id" to correlationIdAttributeValue,
                "DataProduct" to dataProductAttributeValue)

            val getItemRequest = GetItemRequest().apply {
                tableName = "data_pipeline_metadata"
                key = primaryKey
            }
            val result = amazonDynamoDB.getItem(getItemRequest)
            val item = result.item
            val status = item["Status"]
            status.shouldNotBeNull()
            status.s shouldBe "COMPLETED"
        }

        "Correct messages sent" {
            val received = queueMessages(snapshotSenderQueueUrl)
                .map(Message::getBody)
                .map { Gson().fromJson(it, JsonObject::class.java) }
            received shouldHaveSize 39
            val pathValues = received.mapNotNull { it.remove("s3_full_folder") }.map(JsonElement::getAsString).sorted()

            val expected = expectedExports()

            pathValues shouldContainExactly expected

            val (noFilesExportedMessages, filesExportedMessages) = received.map(JsonObject::toString).partition {
                it.contains("files_exported")
            }

            filesExportedMessages shouldHaveSize 38

            filesExportedMessages.filter {
                it.contains(S3_EXPORT_CORRELATION_ID)
            }.forEach {
                it shouldMatchJson """{
                        "shutdown_flag":"false",
                        "correlation_id":"s3-export",
                        "topic_name":"db.database.collection",
                        "export_date":"2020-07-06",
                        "reprocess_files":"true",
                        "snapshot_type":"full"
                    }"""
            }

            filesExportedMessages.filter {
                it.contains("equality-export")
            }.forEach {
                it shouldMatchJson """{
                        "shutdown_flag":"false",
                        "correlation_id":"equality-export",
                        "topic_name":"data.equality",
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
                    "snapshot_type": "full",
                    "export_date": "2020-07-06"
                }""")
        }

        "Correct data egress messages sent" {
            val received = queueMessages(dataEgressQueueUrl)
                    .map(Message::getBody)
                    .map { Gson().fromJson(it, JsonObject::class.java) }

            received shouldHaveSize 2


            val firstExpected = """{
               "s3": {
                   "object": {
                       "key": "output/db.database.collection-/pipeline_success.flag"
                   }
               }
            }"""

            val secondExpected = """{
               "s3": {
                   "object": {
                       "key": "equality/data.equality-/pipeline_success.flag"
                   }
               }
            }"""

            received[0].asString shouldMatchJson firstExpected
            received[1].asString shouldMatchJson secondExpected
        }


        "It should send the monitoring message" {
            validateQueueMessage(monitoringQueueUrl, """{
                    "severity": "Critical",
                    "notification_type": "Information",
                    "slack_username": "HTME",
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
            val correlationIdAttributeValue = AttributeValue().apply { s = "empty-export" }
            val collectionNameAttributeValue = AttributeValue().apply { s = "db.database.empty" }
            val primaryKey = primaryKeyMap(correlationIdAttributeValue, collectionNameAttributeValue)
            val getItemRequest = getItemRequest(primaryKey)
            val result = amazonDynamoDB.getItem(getItemRequest)
            val item = result.item
            item.shouldNotBeNull()
            item["CollectionStatus"]?.s shouldBe "Exported"
            item["FilesExported"]?.n shouldBe "0"
        }

        "dynamoDB should have blocked topic record" {
            val correlationIdAttributeValue = AttributeValue().apply { s = "blocked_topic" }
            val collectionNameAttributeValue = AttributeValue().apply { s = "db.blocked.topic" }
            val primaryKey = primaryKeyMap(correlationIdAttributeValue, collectionNameAttributeValue)
            val getItemRequest = getItemRequest(primaryKey)
            val result = amazonDynamoDB.getItem(getItemRequest)
            val item = result.item
            item shouldNotBe null
            val status = item["CollectionStatus"]?.s
            val expectedCollectionStatus = "Blocked_Topic"
            status shouldBe expectedCollectionStatus
        }

        "dynamoDB has table unavailable record" {
            val correlationIdAttributeValue = AttributeValue().apply { s = "table-unavailable" }
            val collectionNameAttributeValue = AttributeValue().apply { s = "does.not.exist" }
            val primaryKey = primaryKeyMap(correlationIdAttributeValue, collectionNameAttributeValue)
            val getItemRequest = getItemRequest(primaryKey)
            val result = amazonDynamoDB.getItem(getItemRequest)
            val item = result.item
            val status = item["CollectionStatus"]?.s
            val expectedCollectionStatus = "Table_Unavailable"
            status shouldBe expectedCollectionStatus
        }

        "dynamoDB has equality record" {
            val correlationIdAttributeValue = AttributeValue().apply { s = "equality-export" }
            val collectionNameAttributeValue = AttributeValue().apply { s = "data.equality" }
            val primaryKey = primaryKeyMap(correlationIdAttributeValue, collectionNameAttributeValue)
            val getItemRequest = getItemRequest(primaryKey)
            val result = amazonDynamoDB.getItem(getItemRequest)
            val item = result.item
            val status = item["CollectionStatus"]?.s
            val expectedCollectionStatus = "Exported"
            status shouldBe expectedCollectionStatus
        }

    }

    private fun validateIds(records: List<JsonObject>, idExtractor: (JsonObject) -> JsonObject) {
        records.size shouldBe 10_000
        records.asSequence().map(idExtractor)
            .map { if (it["record_id"] != null) it["record_id"] else it["d_oid"] }
            .map(JsonElement::getAsJsonPrimitive)
            .map(JsonPrimitive::getAsString).sorted()
            .forEachIndexed { index, id -> id shouldBe String.format("%05d", index) }
    }

    private suspend fun validateMetric(resource: String, expected: String) {
        val response = client.get<JsonObject>("http://prometheus:9090/api/v1/query?query=$resource")
        val results = response["data"].asJsonObject["result"].asJsonArray
        results.size() shouldBe 1
        val result = results[0].asJsonObject["value"].asJsonArray[1].asJsonPrimitive.asString
        result shouldBe expected
    }

    private fun validateQueueMessage(queueUrl: String, expectedMessage: String) {
        val received = queueMessages(queueUrl)
            .map(Message::getBody)
            .map { Gson().fromJson(it, JsonObject::class.java) }
            .mapNotNull { it["Message"] }

        received shouldHaveSize 1
        received.first().asString shouldMatchJson expectedMessage
    }

    companion object {
        private val client = HttpClient {
            install(JsonFeature) {
                serializer = GsonSerializer {
                    setPrettyPrinting()
                }
            }
        }

        private const val S3_EXPORT_CORRELATION_ID = "s3-export"

        private val applicationContext by lazy { AnnotationConfigApplicationContext(TestConfiguration::class.java) }
        private val amazonS3 by lazy { applicationContext.getBean(AmazonS3::class.java) }
        private val amazonDynamoDB by lazy { applicationContext.getBean(AmazonDynamoDB::class.java) }
        private val amazonSqs by lazy { applicationContext.getBean(AmazonSQS::class.java) }
        private val keyService by lazy { applicationContext.getBean(KeyService::class.java) }
        private val cipherService by lazy { applicationContext.getBean(CipherService::class.java) }

        private const val snapshotSenderQueueUrl = "http://aws:4566/000000000000/integration-queue.fifo"
        private const val dataEgressQueueUrl = "http://aws:4566/000000000000/egress-queue.fifo"
        private const val adgQueueUrl = "http://aws:4566/000000000000/trigger-adg-subscriber"
        private const val monitoringQueueUrl = "http://aws:4566/000000000000/monitoring-subscriber"

        private fun InputStream.copyToByteArray(): ByteArray = this.copyToByteArrayOutputStream().toByteArray()

        private fun InputStream.copyToByteArrayOutputStream() =
            use { inputStream -> ByteArrayOutputStream().also { inputStream.copyTo(it) } }

        private fun List<ByteArrayOutputStream>.lines() =
            this.map(ByteArrayOutputStream::toByteArray)
                .map(ByteArray::decodeToString)
                .toList()
                .flatMap { it.split("\n") }
                .filter(String::isNotBlank).asSequence()


        private fun s3BucketObjects(bucket: String) = s3BucketKeys(bucket).map { amazonS3.getObject(bucket, it) }
        private fun s3BucketKeys(bucket: String) = s3Objects(bucket).objectSummaries.map(S3ObjectSummary::getKey)
        private fun s3Objects(bucket: String) = amazonS3.listObjects(bucket)

        private fun primaryKeyMap(correlationIdAttributeValue: AttributeValue,
                                  collectionNameAttributeValue: AttributeValue) =
            mapOf("CorrelationId" to correlationIdAttributeValue, "CollectionName" to collectionNameAttributeValue)

        private fun decrypting(key: String, initializationVector: String, encrypted: ByteArray): InputStream =
            SecretKeySpec(Base64.getDecoder().decode(key), "AES").let { keySpec ->
                cipherService.decryptingCipher(keySpec, Base64.getDecoder().decode(initializationVector))
                    .let { cipher ->
                        CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.BZIP2,
                            CipherInputStream(ByteArrayInputStream(encrypted), cipher))
                    }
            }

        private fun getItemRequest(primaryKey: Map<String, AttributeValue>) = GetItemRequest().apply {
            tableName = "UCExportToCrownStatus"
            key = primaryKey
        }

        private tailrec fun queueMessages(queueUrl: String, accumulated: List<Message> = listOf()): List<Message> {
            val messages = amazonSqs.receiveMessage(queueUrl).messages

            if (messages == null || messages.isEmpty()) {
                return accumulated
            }
            messages.forEach { deleteMessage(queueUrl, it) }
            return queueMessages(queueUrl, accumulated + messages)
        }

        private fun deleteMessage(queueUrl: String, it: Message) = amazonSqs.deleteMessage(queueUrl, it.receiptHandle)

        private fun expectedExports(): List<String> =
            listOf(
                "equality/data.equality-000-128-000001.txt.bz2.enc",
                "equality/data.equality-000-128-000002.txt.bz2.enc",
                "equality/data.equality-000-128-000003.txt.bz2.enc",
                "equality/data.equality-000-128-000004.txt.bz2.enc",
                "equality/data.equality-000-128-000005.txt.bz2.enc",
                "equality/data.equality-000-128-000006.txt.bz2.enc",
                "equality/data.equality-000-128-000007.txt.bz2.enc",
                "equality/data.equality-000-128-000008.txt.bz2.enc",
                "equality/data.equality-000-128-000009.txt.bz2.enc",
                "equality/data.equality-128-000-000001.txt.bz2.enc",
                "equality/data.equality-128-000-000002.txt.bz2.enc",
                "equality/data.equality-128-000-000003.txt.bz2.enc",
                "equality/data.equality-128-000-000004.txt.bz2.enc",
                "equality/data.equality-128-000-000005.txt.bz2.enc",
                "equality/data.equality-128-000-000006.txt.bz2.enc",
                "equality/data.equality-128-000-000007.txt.bz2.enc",
                "equality/data.equality-128-000-000008.txt.bz2.enc",
                "equality/data.equality-128-000-000009.txt.bz2.enc",
                "output/db.database.collection-000-040-000001.txt.bz2.enc",
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
            listOf("equality/data.equality-000-128-000000.csv",
                "equality/data.equality-000-128-000001.csv",
                "equality/data.equality-000-128-000002.csv",
                "equality/data.equality-000-128-000003.csv",
                "equality/data.equality-000-128-000004.csv",
                "equality/data.equality-000-128-000005.csv",
                "equality/data.equality-000-128-000006.csv",
                "equality/data.equality-000-128-000007.csv",
                "equality/data.equality-000-128-000008.csv",
                "equality/data.equality-128-000-000000.csv",
                "equality/data.equality-128-000-000001.csv",
                "equality/data.equality-128-000-000002.csv",
                "equality/data.equality-128-000-000003.csv",
                "equality/data.equality-128-000-000004.csv",
                "equality/data.equality-128-000-000005.csv",
                "equality/data.equality-128-000-000006.csv",
                "equality/data.equality-128-000-000007.csv",
                "equality/data.equality-128-000-000008.csv",
                "output/db.database.collection-000-040-000000.csv",
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
