package app.batch

import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.PutObjectResult
import com.nhaarman.mockitokotlin2.*
import io.prometheus.client.Counter
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.retry.annotation.EnableRetry
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.File

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [StreamingManifestWriter::class])
@EnableRetry
@TestPropertySource(properties = [
    "manifest.retry.maxAttempts=10",
    "manifest.retry.delay=1",
    "manifest.retry.multiplier=1"
])
class StreamingManifestWriterTest {

    @Before
    fun before() {
        reset(counter)
    }

    @Autowired
    private lateinit var streamingManifestWriter: StreamingManifestWriter

    @MockBean
    private lateinit var counter: Counter

    @Test
    fun writeManifestDoesNotRetryOnSuccess() {
        val amazonS3 = mock<AmazonS3> {
            on { putObject(any()) }  doReturn PutObjectResult()
        }
        streamingManifestWriter.sendManifest(amazonS3, file(),"bucket", "prefix")
        verify(amazonS3, times(1)).putObject(any())
        verifyNoMoreInteractions(amazonS3)
        verifyZeroInteractions(counter)
    }

    @Test
    fun writeManifestGivesUpAfterMaxAttempts() {
        val amazonS3 = mock<AmazonS3> {
            on { putObject(any()) } doThrow SdkClientException("Error")
        }

        try {
            streamingManifestWriter.sendManifest(amazonS3, file(),"bucket", "prefix")
        }
        catch (e: Exception) {
            // do nothing
        }
        verify(amazonS3, times(10)).putObject(any())
        verify(counter, times(10)).inc()
    }

    @Test
    fun writeManifestDoesRetryOnFailure() {
        val amazonS3 = mock<AmazonS3> {
            on { putObject(any()) } doThrow SdkClientException("Error") doReturn PutObjectResult()
        }
        try {
            streamingManifestWriter.sendManifest(amazonS3, file(),"bucket", "prefix")
        }
        catch (e: Exception) {
            // do nothing
        }

        verify(amazonS3, times(2)).putObject(any())
        verify(counter, times(1)).inc()
    }

    private fun file() = File("src/test/kotlin/app/batch/StreamingManifestWriterTest.kt")
}
