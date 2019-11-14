package app.batch

import app.domain.ManifestRecord
import app.domain.Record
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.PutObjectRequest
import com.nhaarman.mockitokotlin2.*
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@ActiveProfiles("phoneyDataKeyService", "phoneyCipherService", "unitTest", "outputToS3")
@SpringBootTest
@TestPropertySource(properties = [
    "directory.output=ephemera",
    "output.batch.size.max.bytes=100000",
    "source.table.name=ucdata",
    "compress.output=true",
    "encrypt.output=true",
    "data.table.name=ucfs-data",
    "column.family=topic",
    "topic.name=db.core.addressDeclaration",
    "aws.region=eu-west-1",
    "s3.bucket=not_set",
    "s3.prefix.folder=not_set",
    "identity.keystore=resources/identity.jks",
    "trust.keystore=resources/truststore.jks",
    "identity.store.password=changeit",
    "identity.key.password=changeit",
    "trust.store.password=changeit",
    "identity.store.alias=cid",
    "hbase.zookeeper.quorum=hbase",
    "s3.manifest.prefix.folder=test-manifest-exporter"
])

class S3DirectoryWriterTest {


    @Before
    fun setUp() {
        Mockito.reset(s3Client)
    }

    @Test
    fun testWriteData() {

        val listOfLists: MutableList<MutableList<Record>> = mutableListOf()
        var total = 0

        for (i in 1..10) {
            val list: MutableList<Record> = mutableListOf()
            for (j in 1..10) {
                val token = "[%03d/%04d]".format(i, j)
                val item = token.repeat(j * (11 - i) * 10)
                list.add(Record(item, ManifestRecord("id", 0, "db", "collection", "EXPORT")))
                total += item.length
            }
            listOfLists.add(list)
        }
        listOfLists.forEach {
            s3DirectoryWriter.write(it)
        }

        s3DirectoryWriter.writeOutput()
        Mockito.verify(s3Client, Mockito.times(8))
            .putObject(ArgumentMatchers.any(PutObjectRequest::class.java))
    }

    @Test
    fun testManifestFileFormat() {
        val expected = "test-manifest-exporter/db.core.addressDeclaration-000004.csv"
        val actual = s3DirectoryWriter.generateManifestFileFormat()
        assertEquals(expected, actual)
    }

    @Test
    fun testCSVManifestGeneration() {
        val manifestRecord1 = ManifestRecord("\"_id\":{\"declarationId\": \"1234567890\"}", 100000000, "dbwithcomma,", "collectionwithdoublequote\"", "EXPORT")
        val manifestRecord2 = ManifestRecord("id2", 200000000, "db2", "collection2", "EXPORT")
        val list = mutableListOf<ManifestRecord>()
        list.add(manifestRecord1)
        list.add(manifestRecord2)
        val actual = s3DirectoryWriter.generateEscapedCSV(list)
        val expected = "\"\"\"_id\"\":{\"\"declarationId\"\": \"\"1234567890\"\"}\",100000000,\"dbwithcomma,\",\"collectionwithdoublequote\"\"\",EXPORT\n" +
            "id2,200000000,db2,collection2,EXPORT"
        assertEquals(expected, actual)
    }

    @Test
    fun testManifest() {
        val manifestRecord1 = ManifestRecord("id1", 100000000, "db1", "collection1", "EXPORT")
        val manifestRecord2 = ManifestRecord("id2", 200000000, "db2", "collection2", "EXPORT")
        val list = mutableListOf<ManifestRecord>()
        list.add(manifestRecord1)
        list.add(manifestRecord2)
        s3DirectoryWriter.writeManifest(list)
        Mockito.verify(s3Client, Mockito.times(1))
            .putObject(ArgumentMatchers.any(PutObjectRequest::class.java))
    }

    @Test
    fun testManifestLogsException(){

        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)
        doThrow(RuntimeException()).whenever(s3DirectoryWriter).generateManifestFileFormat()
        val manifestRecord1 = ManifestRecord("id1", 100000000, "db1", "collection1", "EXPORT")
        val manifestRecord2 = ManifestRecord("id2", 200000000, "db2", "collection2", "EXPORT")
        val list = mutableListOf<ManifestRecord>()
        list.add(manifestRecord1)
        list.add(manifestRecord2)
        s3DirectoryWriter.writeManifest(list)
        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(1)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        Assert.assertTrue(formattedMessages.contains("Exception while writing ids: 'id1:id2' of db: 'db1, collection: collection1' to manifest files in S3"))

    }

    @Test
    fun testManifestFileMetadataGeneration() {
        val manifestFileName = "test-manifest-exporter/db.core.addressDeclaration.csv"
        val actual = s3DirectoryWriter.generateManifestFileMetadata(manifestFileName, 1024)
        assertEquals("binary/octetstream", actual.contentType)
        assertEquals(manifestFileName, actual.userMetadata.get("x-amz-meta-title"))
        assertEquals(1024, actual.contentLength)

    }

    @SpyBean
    private lateinit var s3DirectoryWriter: S3DirectoryWriter

    @Autowired
    private lateinit var s3Client: AmazonS3

    companion object {
        val logger: Logger = LoggerFactory.getLogger(S3DirectoryWriterTest::class.toString())
    }

}
