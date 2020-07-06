import app.configuration.LocalStackConfiguration
import com.amazonaws.services.s3.AmazonS3
import org.junit.Assert.assertEquals
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
}



