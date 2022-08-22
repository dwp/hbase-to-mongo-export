package app.batch

import app.configuration.CompressionInstanceProvider
import app.domain.DataKeyResult
import app.domain.ManifestRecord
import app.domain.Record
import app.exceptions.DataKeyServiceUnavailableException
import app.services.*
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sqs.AmazonSQS
import com.nhaarman.mockitokotlin2.*
import io.prometheus.client.Counter
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.jupiter.api.assertThrows
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.security.SecureRandom

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [S3StreamingWriter::class])
@TestPropertySource(properties = [
    "output.batch.size.max.bytes=100000",
    "s3.manifest.bucket=manifests",
    "s3.manifest.prefix.folder=manifestprefix",
    "s3.prefix.folder=prefix",
    "topic.name=db.database.collection",
    "snapshot.type=incremental",
])
class S3StreamingWriterTest {

    @Before
    fun before() {
        reset(recordCounter)
        reset(byteCounter)
    }

    @Test
    fun testExportStatusServiceIncrementsExportedCount() {
        val byteArrayOutputStream = ByteArrayOutputStream()
        val os = CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, byteArrayOutputStream)
        given(compressionInstanceProvider.compressionExtension()).willReturn("bz2")
        given(compressionInstanceProvider.compressorOutputStream(any())).willReturn(os)

        val dataKeyResult = dataKeyResult()

        given(keyService.batchDataKey()).willReturn(dataKeyResult)
        given(cipherService.cipherOutputStream(any(), any(), any()))
                .willReturn(byteArrayOutputStream)
        val dbObject = "dbObject"
        val manifestRecord = manifestRecord()
        val record = Record(dbObject, manifestRecord)

        val recordCounterChild = mock<Counter.Child>()
        val byteCounterChild = mock<Counter.Child>()
        given(recordCounter.labels(any())).willReturn(recordCounterChild)
        given(byteCounter.labels(any())).willReturn(byteCounterChild)

        s3StreamingWriter.write(mutableListOf(record))
        s3StreamingWriter.writeOutput()
        verify(exportStatusService, times(1)).incrementExportedCount(any())
        verify(recordCounterChild, times(1)).inc(1.toDouble())
        verify(byteCounterChild, times(1)).inc("dbObject\n".length.toDouble())
        val key = "prefix/db.database.collection-${Int.MIN_VALUE}-${Int.MAX_VALUE}-000001.txt.bz2.enc"
        verify(snapshotSenderMessagingService, times(1)).notifySnapshotSender(key)
        verifyZeroInteractions(failedBatchPutCounter)
        verifyZeroInteractions(failedManifestPutCounter)
        verifyZeroInteractions(dksNewDataKeyFailuresCounter)
    }

    @Test
    fun testDbObjectWrittenFaithfully() {

        val byteArrayOutputStream = ByteArrayOutputStream()
        val os = CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, byteArrayOutputStream)
        given(compressionInstanceProvider.compressionExtension()).willReturn("bz2")
        given(compressionInstanceProvider.compressorOutputStream(any())).willReturn(os)

        val dataKeyResult = dataKeyResult()

        given(keyService.batchDataKey()).willReturn(dataKeyResult)
        given(cipherService.cipherOutputStream(any(), any(), any()))
                .willReturn(byteArrayOutputStream)
        val dbObject = "dbObject"
        val manifestRecord = manifestRecord()
        val record = Record(dbObject, manifestRecord)
        s3StreamingWriter.write(mutableListOf(record))
        s3StreamingWriter.writeOutput()
        val written = byteArrayOutputStream.toByteArray()
        val decompress = CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.BZIP2, ByteArrayInputStream(written))
        val sink = ByteArray(9)
        BufferedInputStream(decompress).read(sink)
        val actual = String(sink)
        val expected = "$dbObject\n"
        Assert.assertEquals(expected, actual)
        verifyZeroInteractions(failedBatchPutCounter)
        verifyZeroInteractions(failedManifestPutCounter)
        verifyZeroInteractions(dksNewDataKeyFailuresCounter)
    }

    @Test
    fun testDksFailuresAreRecorded() {
        given(keyService.batchDataKey()).willThrow(DataKeyServiceUnavailableException("Error"))

        assertThrows<DataKeyServiceUnavailableException> {
            s3StreamingWriter.write(mutableListOf(Record("dbObject", manifestRecord())))
            s3StreamingWriter.writeOutput()
        }

        verifyZeroInteractions(failedBatchPutCounter)
        verifyZeroInteractions(failedManifestPutCounter)
        verify(dksNewDataKeyFailuresCounter, times(1)).inc()
        verifyNoMoreInteractions(dksNewDataKeyFailuresCounter)
    }

    @Test
    fun testObjectsAreChunkedAccordingToMaxChunkSize() {
        val dataKeyResult = dataKeyResult()
        val byteArrayOutputStream = ByteArrayOutputStream()
        given(compressionInstanceProvider.compressionExtension()).willReturn("bz2")
        val ongoingStubbing = given(compressionInstanceProvider.compressorOutputStream(any()))
                .willReturn(CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, byteArrayOutputStream))

        for (i in 1..10) {
            ongoingStubbing.willReturn(CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, byteArrayOutputStream))
        }

        given(keyService.batchDataKey()).willReturn(dataKeyResult)
        given(cipherService.cipherOutputStream(any(), any(), any()))
                .willReturn(byteArrayOutputStream)
        val listOfLists: MutableList<MutableList<Record>> = mutableListOf()
        var total = 0

        for (i in 1..10) {
            val list: MutableList<Record> = mutableListOf()
            for (j in 1..10) {
                val token = "[%03d/%04d]".format(i, j)
                val item = token.repeat(j * (11 - i) * 10)
                list.add(Record(item, ManifestRecord("", 0, "", "", "", "", "", "")))
                total += item.length
            }
            listOfLists.add(list)
        }
        given(recordCounter.labels(any())).willReturn(mock())
        given(byteCounter.labels(any())).willReturn(mock())

        listOfLists.forEach {
            s3StreamingWriter.write(it)
        }

        s3StreamingWriter.writeOutput()
        verify(s3StreamingWriter, times(5)).writeOutput()
        verify(s3ObjectService, times(4)).putObject(any(), any())
        verify(streamingManifestWriter, times(4)).sendManifest(any(), any(), any(), any())
        verifyZeroInteractions(failedBatchPutCounter)
        verifyZeroInteractions(failedManifestPutCounter)
        verifyZeroInteractions(dksNewDataKeyFailuresCounter)
   }

    @Test
    fun testManifestWrittenFaithfully() {
        val dataKeyResult = dataKeyResult()
        val byteArrayOutputStream = ByteArrayOutputStream()
        given(keyService.batchDataKey()).willReturn(dataKeyResult)
        given(cipherService.cipherOutputStream(any(), any(), any()))
                .willReturn(byteArrayOutputStream)

        val sink = ByteArrayOutputStream()
        val os = CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, sink)
        given(compressionInstanceProvider.compressionExtension()).willReturn("bz2")
        given(compressionInstanceProvider.compressorOutputStream(any())).willReturn(os)
        val dbObject = "dbObject"
        val manifestRecord = manifestRecord()
        val record = Record(dbObject, manifestRecord)
        given(recordCounter.labels(any())).willReturn(mock())
        given(byteCounter.labels(any())).willReturn(mock())
        s3StreamingWriter.write(mutableListOf(record))
        s3StreamingWriter.writeOutput()
        val s3Captor = argumentCaptor<AmazonS3>()
        val bucketCaptor = argumentCaptor<String>()
        val prefixCaptor = argumentCaptor<String>()
        verify(streamingManifestWriter, times(1)).sendManifest(s3Captor.capture(), any(), bucketCaptor.capture(), prefixCaptor.capture())
        Assert.assertEquals(s3, s3Captor.firstValue)
        Assert.assertEquals("manifests", bucketCaptor.firstValue)
        Assert.assertEquals("manifestprefix", prefixCaptor.firstValue)
        verifyZeroInteractions(failedBatchPutCounter)
        verifyZeroInteractions(failedManifestPutCounter)
        verifyZeroInteractions(dksNewDataKeyFailuresCounter)
    }

    @Test
    fun testSanitiseTableName() {
        val testTableNames = listOf(
            "simple-test-table",
            "simple-test-Table",
            "simple-Test-Table",
            "simple-testTable",
            "simple-TestTable",
            "simpleTest-table",
            "simpleTest-Table"
        )

        testTableNames.forEach { t ->
            val testCaseTable = s3StreamingWriter.sanitiseTableName(t)
            Assert.assertEquals("simpleTestTable", testCaseTable)
        }
    }

    private fun dataKeyResult() = DataKeyResult("dataKeyEncryptionKeyId",
            "plaintextDataKey",
            "ciphertextDataKey")

    private fun manifestRecord() = ManifestRecord("id",
            123.toLong(),
            "db",
            "collection",
            "source",
            "externalSource", "", "")

    @SpyBean
    private lateinit var s3StreamingWriter: S3StreamingWriter

    @MockBean
    private lateinit var compressionInstanceProvider: CompressionInstanceProvider

    @MockBean
    private lateinit var cipherService: CipherService

    @MockBean
    private lateinit var keyService: KeyService

    @MockBean
    private lateinit var s3ObjectService: S3ObjectService

    @MockBean
    private lateinit var secureRandom: SecureRandom

    @MockBean
    private lateinit var s3: AmazonS3

    @MockBean
    private lateinit var streamingManifestWriter: StreamingManifestWriter

    @MockBean(name = "recordCounter")
    private lateinit var recordCounter: Counter

    @MockBean(name = "byteCounter")
    private lateinit var byteCounter: Counter

    @MockBean(name = "failedBatchPutCounter")
    private lateinit var failedBatchPutCounter: Counter

    @MockBean(name = "failedManifestPutCounter")
    private lateinit var failedManifestPutCounter: Counter

    @MockBean(name = "dksNewDataKeyFailuresCounter")
    private lateinit var dksNewDataKeyFailuresCounter: Counter

    @MockBean
    private lateinit var amazonDynamoDb: AmazonDynamoDB

    @MockBean
    private lateinit var amazonSQS: AmazonSQS

    @MockBean
    private lateinit var exportStatusService: ExportStatusService

    @MockBean
    private lateinit var snapshotSenderMessagingService: MessagingService

}
