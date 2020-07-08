import app.configuration.LocalStackConfiguration
import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import io.kotlintest.shouldBe
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringRunner
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.Reader

@RunWith(SpringRunner::class)
@ContextConfiguration(classes = [LocalStackConfiguration::class])
@ActiveProfiles("localstackConfiguration")
class S3WriterIntegrationTest {

    @Autowired
    private lateinit var s3Client: AmazonS3

    @Value("\${s3.manifest.bucket:manifestbucket}")
    private lateinit var s3BucketName: String

    @Value("\${s3.manifest.prefix.folder:test-manifest-exporter}")
    private lateinit var s3ManifestPrefixFolder: String

    @Test
    fun testMethod() {
        val oid = "\$oid"
        val date = "\$date"
        val expected = """
            |"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|1426854205183|penalties-and-deductions|sanction|EXPORT|V4|"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|MONGO_INSERT
            |"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|1544799662000|penalties-and-deductions|sanction|EXPORT|V4|"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|TYPE_NOT_SET
            |"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|1544799662000|penalties-and-deductions|sanction|EXPORT|V4|"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|TYPE_NOT_SET
            |"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|1544799662000|penalties-and-deductions|sanction|EXPORT|V4|"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|TYPE_NOT_SET
            |"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|1544799662000|penalties-and-deductions|sanction|EXPORT|V4|"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|TYPE_NOT_SET
            |"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|1544799662000|penalties-and-deductions|sanction|EXPORT|V4|"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|TYPE_NOT_SET
            |"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|1544799662000|penalties-and-deductions|sanction|EXPORT|V4|"{""_lastModifiedDateTime"":{""$date"":""2013-08-05T02:10:19.887Z""},""_removedDateTime"":{""$date"":""2011-08-05T02:10:19.887Z""},""createdDateTime"":{""$date"":""2010-08-05T02:10:19.000Z""},""declarationId"":1234,""someId"":""RANDOM_GUID""}"|TYPE_NOT_SET
            |"{""$oid"":""ID_CONSTRUCTED_FROM_NATIVE_MONGO""}"|1426854205183|penalties-and-deductions|sanction|EXPORT|V4|ID_CONSTRUCTED_FROM_NATIVE_MONGO|MONGO_INSERT
            """.trimMargin()

        val summaries = s3Client.listObjectsV2(s3BucketName, s3ManifestPrefixFolder).objectSummaries
        val list = summaries.map {
            val objectContent = s3Client.getObject(it.bucketName, it.key).objectContent
            BufferedReader(InputStreamReader(objectContent) as Reader?).use { it.readText().trim() }
        }
        val joinedContent = list.joinToString("\n")
        assertEquals(expected, joinedContent)
    }

    @Test
    fun exportStatusIsExported() {
        val dynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://aws:4566/",
                        "eu-west-2"))
                .withClientConfiguration(ClientConfiguration().withProtocol(Protocol.HTTP))
                .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials("access-key", "secret-key")))
                .build()
        val correlationIdAttributeValue = AttributeValue().apply { s = "integration_test_correlation_id" }
        val collectionNameAttributeValue = AttributeValue().apply { s = "db.penalties-and-deductions.sanction" }
        val primaryKey = mapOf("CorrelationId" to correlationIdAttributeValue,
                "CollectionName" to collectionNameAttributeValue)

        val getItemRequest = GetItemRequest().apply {
            tableName = "UCExportToCrownStatus"
            key = primaryKey
        }
        val result = dynamoDB.getItem(getItemRequest)
        val item = result.item
        val status = item["CollectionStatus"]
        val filesExported = item["FilesExported"]
        val filesSent = item["FilesSent"]
        status?.s shouldBe "Exported"
        filesExported?.n shouldBe "7"
        filesSent?.n shouldBe "0"
    }

    @Test
    fun messageSentToSnapshotSender() {
        val sqs = AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://aws:4566/", "eu-west-2"))
                .withClientConfiguration(ClientConfiguration().withProtocol(Protocol.HTTP))
                .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials("access-key", "secret-key")))
                .build()

        for (i in 1 .. 7) {
            val messageResult = sqs.receiveMessage("http://aws:4566/000000000000/integration-queue")
            assertNotNull(messageResult)
            val messages = messageResult?.messages
            val expectedMessageBody = """
            {
               "shutdown_flag": "false",
               "correlation_id": "integration_test_correlation_id",
               "topic_name": "db.penalties-and-deductions.sanction",
               "export_date": "2020-07-06",
               "reprocess_files": "true",
               "s3_full_folder": "test-exporter/db.penalties-and-deductions.sanction-045-050-00000${i}.txt.bz2.enc"
            }
            """.trimIndent()

            assertEquals(expectedMessageBody, messages?.get(0)?.body)

        }
    }

}
