package app.services.impl

import app.domain.DataKeyResult
import app.domain.EncryptingOutputStream
import app.services.S3ObjectService
import com.amazonaws.services.s3.AmazonS3
import com.nhaarman.mockitokotlin2.*
import io.prometheus.client.Counter
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.retry.annotation.EnableRetry
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [S3ObjectServiceImpl::class])
@EnableRetry
@TestPropertySource(properties = [
    "s3.retry.maxAttempts=5",
    "s3.retry.delay=1",
    "s3.retry.multiplier=1"
])
class S3ObjectServiceImplTest {

    @Before
    fun beforeEach() {
        reset(amazonS3)
    }

    @Test
    fun givesUpAfterMaxRetries() {
        given(amazonS3.putObject(any())).willThrow(RuntimeException("ERROR"))
        try {
            s3ObjectService.putObject("key", encryptingOutputStream())
            fail("Exception expected")
        } catch (e: Exception) {
            // just catch it
        }
        verify(amazonS3, times(5)).putObject(any())
        verify(counter, times(5)).inc()
    }


    @Test
    fun retriesUntilSuccessful() {
        given(amazonS3.putObject(any()))
            .willThrow(RuntimeException("ERROR 1"))
            .willThrow(RuntimeException("ERROR 2"))
            .willThrow(RuntimeException("ERROR 3"))
            .willReturn(mock())
        s3ObjectService.putObject("key", encryptingOutputStream())
        verify(amazonS3, times(4)).putObject(any())
        verify(counter, times(3)).inc()
    }

    private fun dataKeyResult(): DataKeyResult =
        DataKeyResult("dataKeyEncryptionKeyId", "plaintextDatakey", "ciphertextDataKey")

    private fun encryptingOutputStream(): EncryptingOutputStream =
        EncryptingOutputStream(BufferedOutputStream(ByteArrayOutputStream()),
            ByteArrayOutputStream(), dataKeyResult(), "initialisationVector", mock(), mock())

    @Autowired
    private lateinit var s3ObjectService: S3ObjectService

    @MockBean
    private lateinit var amazonS3: AmazonS3

    @MockBean
    private lateinit var counter: Counter
}
