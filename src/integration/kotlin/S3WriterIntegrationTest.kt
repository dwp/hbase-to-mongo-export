import app.configuration.S3DummyConfiguration
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
@ContextConfiguration(classes = [S3DummyConfiguration::class])
@ActiveProfiles("dummyS3Client")
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
        val expected = """
            |"{""someId"":""RANDOM_GUID"",""declarationId"":1234}",1544799662000,penalties-and-deductions,sanction,EXPORT,V4,"{""someId"":""RANDOM_GUID"",""declarationId"":1234}"
            |"{""someId"":""RANDOM_GUID"",""declarationId"":1234}",1544799662000,penalties-and-deductions,sanction,EXPORT,V4,"{""someId"":""RANDOM_GUID"",""declarationId"":1234}"
            |"{""someId"":""RANDOM_GUID"",""declarationId"":1234}",1544799662000,penalties-and-deductions,sanction,EXPORT,V4,"{""someId"":""RANDOM_GUID"",""declarationId"":1234}"
            |"{""someId"":""RANDOM_GUID"",""declarationId"":1234}",1544799662000,penalties-and-deductions,sanction,EXPORT,V4,"{""someId"":""RANDOM_GUID"",""declarationId"":1234}"
            |"{""someId"":""RANDOM_GUID"",""declarationId"":1234}",1544799662000,penalties-and-deductions,sanction,EXPORT,V4,"{""someId"":""RANDOM_GUID"",""declarationId"":1234}"
            |"{""someId"":""RANDOM_GUID"",""declarationId"":1234}",1544799662000,penalties-and-deductions,sanction,EXPORT,V4,"{""someId"":""RANDOM_GUID"",""declarationId"":1234}"
            |"{""someId"":""RANDOM_GUID"",""declarationId"":1234}",1544799662000,penalties-and-deductions,sanction,EXPORT,V4,"{""someId"":""RANDOM_GUID"",""declarationId"":1234}"
            |"{""$oid"":""ID_CONSTRUCTED_FROM_NATIVE_MONGO""}",1544799662000,penalties-and-deductions,sanction,EXPORT,V4,ID_CONSTRUCTED_FROM_NATIVE_MONGO
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



