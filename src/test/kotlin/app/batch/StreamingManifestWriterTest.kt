package app.batch

import com.amazonaws.AmazonServiceException
import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.PutObjectResult
import com.nhaarman.mockitokotlin2.*
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.retry.annotation.EnableRetry
import org.springframework.test.context.junit4.SpringRunner
import java.io.File

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [StreamingManifestWriter::class])
@EnableRetry
class StreamingManifestWriterTest {

    @Autowired
    private lateinit var streamingManifestWriter: StreamingManifestWriter

    @Test
    fun writeManifestDoesNotRetryOnSuccess() {
        val amazonS3 = mock<AmazonS3> {
            on { putObject(any()) }  doReturn PutObjectResult()
        }

        val file = File("src/test/kotlin/app/batch/StreamingManifestWriterTest.kt")

        streamingManifestWriter.sendManifest(amazonS3, file,"bucket", "prefix")
        verify(amazonS3, times(1)).putObject(any())
        verifyNoMoreInteractions(amazonS3)
    }

    @Test
    fun writeManifestDoesGiveUpOnRepeatedFailure() {
        val amazonS3 = mock<AmazonS3> {
            on { putObject(any()) } doThrow SdkClientException("Error")
        }

        val file = File("src/test/kotlin/app/batch/StreamingManifestWriterTest.kt")

        try {
            streamingManifestWriter.sendManifest(amazonS3, file,"bucket", "prefix")
        }
        catch (e: Exception) {}
        verify(amazonS3, times(5)).putObject(any())
    }

    @Test
    fun writeManifestDoesRetryOnFailure() {
        val amazonS3 = mock<AmazonS3> {
            on { putObject(any()) } doThrow SdkClientException("Error") doReturn PutObjectResult()
        }

        val file = File("src/test/kotlin/app/batch/StreamingManifestWriterTest.kt")

        try {
            streamingManifestWriter.sendManifest(amazonS3, file,"bucket", "prefix")
        }
        catch (e: Exception) {}
        verify(amazonS3, times(2)).putObject(any())
    }
}
