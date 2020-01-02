package app.batch

import app.configuration.CompressionInstanceProvider
import app.domain.DataKeyResult
import app.domain.ManifestRecord
import app.domain.Record
import app.services.CipherService
import app.services.KeyService
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.PutObjectRequest
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.times
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.BDDMockito.given
import org.mockito.Mockito
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.*
import java.security.Key
import java.security.SecureRandom

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [StreamingWriter::class])
@ActiveProfiles("streamingWriter")
@TestPropertySource(properties = [
    "output.batch.size.max.bytes=100000",
    "s3.bucket=exportbucket",
    "s3.manifest.bucket=manifestbucket",
    "s3.manifest.prefix.folder=manifestprefix",
    "s3.prefix.folder=prefix",
    "topic.name=topic"
])

class StreamingWriterTest {

    @MockBean
    private lateinit var compressionInstanceProvider: CompressionInstanceProvider

    @MockBean
    private lateinit var cipherService: CipherService

    @MockBean
    private lateinit var keyService: KeyService

    @MockBean
    private lateinit var secureRandom: SecureRandom

    @MockBean
    private lateinit var s3: AmazonS3

    @MockBean
    private lateinit var streamingManifestWriter: StreamingManifestWriter

    @Test
    fun testDbObjectWrittenFaithfully() {

        val byteArrayOutputStream = ByteArrayOutputStream()
        val os = CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, byteArrayOutputStream)
        given(compressionInstanceProvider.compressionExtension()).willReturn("bz2")
        given(compressionInstanceProvider.compressorOutputStream(any<OutputStream>())).willReturn(os)

        val dataKeyResult = dataKeyResult()

        given(keyService.batchDataKey()).willReturn(dataKeyResult)
        given(cipherService.cipherOutputStream(any<Key>(), any<ByteArray>(), any<OutputStream>()))
                .willReturn(byteArrayOutputStream)
        val dbObject = "dbObject"
        val manifestRecord = manifestRecord()
        val record = Record(dbObject, manifestRecord)
        streamingWriter.write(mutableListOf(record))
        streamingWriter.writeOutput()
        val written = byteArrayOutputStream.toByteArray()
        val decompress = CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.BZIP2, ByteArrayInputStream(written))
        val sink = ByteArray(9)
        BufferedInputStream(decompress).read(sink)
        val actual = String(sink)
        val expected = "$dbObject\n"
        Assert.assertEquals(expected, actual)
    }

    @Test
    fun testObjectsAreChunkedAccordingToMaxChunkSize() {
        val dataKeyResult = dataKeyResult()
        val byteArrayOutputStream = ByteArrayOutputStream()
        given(compressionInstanceProvider.compressionExtension()).willReturn("bz2")
        val ongoingStubbing = given(compressionInstanceProvider.compressorOutputStream(any<OutputStream>()))
                .willReturn(CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, byteArrayOutputStream))

        for (i in 1..10) {
            ongoingStubbing.willReturn(CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, byteArrayOutputStream))
        }

        given(keyService.batchDataKey()).willReturn(dataKeyResult)
        given(cipherService.cipherOutputStream(any<Key>(), any<ByteArray>(), any<OutputStream>()))
                .willReturn(byteArrayOutputStream)
        val listOfLists: MutableList<MutableList<Record>> = mutableListOf()
        var total = 0

        for (i in 1..10) {
            val list: MutableList<Record> = mutableListOf()
            for (j in 1..10) {
                val token = "[%03d/%04d]".format(i, j)
                val item = token.repeat(j * (11 - i) * 10)
                list.add(Record(item, ManifestRecord("", 0, "", "", "", "")))
                total += item.length
            }
            listOfLists.add(list)
        }

        listOfLists.forEach {
            streamingWriter.write(it)
        }

        streamingWriter.writeOutput()
        val putObjectRequest = argumentCaptor<PutObjectRequest>()
        Mockito.verify(streamingWriter, times(5)).writeOutput()
        Mockito.verify(s3, times(4)).putObject(putObjectRequest.capture())
        Mockito.verify(streamingManifestWriter, times(4)).sendManifest(any(), any(), any(), any())
   }

    @Test
    fun testManifestWrittenFaithfully() {
        val dataKeyResult = dataKeyResult()
        val byteArrayOutputStream = ByteArrayOutputStream()
        given(keyService.batchDataKey()).willReturn(dataKeyResult)
        given(cipherService.cipherOutputStream(any<Key>(), any<ByteArray>(), any<OutputStream>()))
                .willReturn(byteArrayOutputStream)

        val sink = ByteArrayOutputStream()
        val os = CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, sink)
        given(compressionInstanceProvider.compressionExtension()).willReturn("bz2")
        given(compressionInstanceProvider.compressorOutputStream(any<OutputStream>())).willReturn(os)
        val dbObject = "dbObject"
        val manifestRecord = manifestRecord()
        val record = Record(dbObject, manifestRecord)
        streamingWriter.write(mutableListOf(record))
        streamingWriter.writeOutput()
        val s3Captor = argumentCaptor<AmazonS3>()
        val bucketCaptor = argumentCaptor<String>()
        val prefixCaptor = argumentCaptor<String>()
        Mockito.verify(streamingManifestWriter, Mockito.times(1))
                .sendManifest(s3Captor.capture(), any<File>(), bucketCaptor.capture(), prefixCaptor.capture())

        Assert.assertEquals(s3, s3Captor.firstValue)
        Assert.assertEquals("manifestbucket", bucketCaptor.firstValue)
        Assert.assertEquals("manifestprefix", prefixCaptor.firstValue)
    }

    private fun dataKeyResult() = DataKeyResult("dataKeyEncryptionKeyId",
            "plaintextDataKey",
            "ciphertextDataKey")

    private fun manifestRecord() = ManifestRecord("id",
            123.toLong(),
            "db",
            "collection",
            "source",
            "externalSource")

    @SpyBean
    private lateinit var streamingWriter: StreamingWriter
}
