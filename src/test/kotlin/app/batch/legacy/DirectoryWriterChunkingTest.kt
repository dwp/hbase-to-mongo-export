package app.batch.legacy

import app.domain.ManifestRecord
import app.domain.Record
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.sqs.AmazonSQS
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.File

@RunWith(SpringRunner::class)
@ActiveProfiles("phoneyDataKeyService", "phoneyCipherService", "unitTest", "outputToDirectory")
@SpringBootTest
@TestPropertySource(properties = [
    "directory.output=ephemera",
    "output.batch.size.max.bytes=100000",
    "data.table.name=ucfs-data",
    "compress.output=false",
    "encrypt.output=false",
    "column.family=topic",
    "topic.name=db.a.b",
    "identity.keystore=resources/identity.jks",
    "trust.keystore=resources/truststore.jks",
    "identity.store.password=changeit",
    "identity.key.password=changeit",
    "trust.store.password=changeit",
    "identity.store.alias=cid",
    "hbase.zookeeper.quorum=hbase",
    "aws.region=eu-west-2",
    "snapshot.sender.sqs.queue.url=http://aws:4566",
    "snapshot.sender.reprocess.files=true",
    "snapshot.sender.shutdown.flag=true",
    "snapshot.sender.export.date=2020-06-05",
    "trigger.snapshot.sender=false",
    "snapshot.type=full"
])
class DirectoryWriterChunkingTest {

    @Before
    fun init() {
        val outputDirectory = File(outputDirectoryPath)
        if (!outputDirectory.isDirectory) {
            outputDirectory.mkdirs()
        }

        outputDirectory.listFiles().forEach {
            it.delete()
        }
    }

    @Test
    fun testChunking() {
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

        /*
         * There are 10 batches each of 10 items (items are represented by their lengths below).
         * Given a max output size of 100 000 we would expect a new file to be opened before
         * each of the starred items below:
         * [
         *  [ *1000,  2000,  3000,  4000,  5000,  6000,  7000,  8000,  9000,  10000], // batch  1
         *  [   900,  1800,  2700,  3600,  4500,  5400,  6300,  7200,  8100,  *9000], // batch  2
         *  [   800,  1600,  2400,  3200,  4000,  4800,  5600,  6400,  7200,   8000], // batch  3
         *  [   700,  1400,  2100,  2800,  3500,  4200,  4900,  5600,  6300,   7000], // batch  4
         *  [   600,  1200,  1800,  2400,  3000,  3600, *4200,  4800,  5400,   6000], // batch  5
         *  [   500,  1000,  1500,  2000,  2500,  3000,  3500,  4000,  4500,   5000], // batch  6
         *  [   400,   800,  1200,  1600,  2000,  2400,  2800,  3200,  3600,   4000], // batch  7
         *  [   300,   600,   900,  1200,  1500,  1800,  2100,  2400,  2700,   3000], // batch  8
         *  [   200,   400,   600,   800,  1000,  1200,  1400,  1600,  1800,   2000], // batch  9
         *  [   100,   200,   300,   400,   500,   600,   700,  *800,   900,   1000]  // batch 10
         * ]
         *
         * And so there should be 4 output files with the following sizes:
         *
         * 1. 95 519
         * 2. 97 525
         * 3. 98 643
         * 4. 10 913
         *
         * (the sizes are the totals of the item sizes in each star delimited set of items above plus 1 byte for the
         *  new line that is written after each of those items).
         */

        listOfLists.forEach {
            directoryWriter.write(it)
        }

        directoryWriter.writeOutput()

        val outputs = File(outputDirectoryPath).listFiles()

        assertEquals(8, outputs.size)

        val expectedSizes = mapOf(
            "db.a.b-000001.metadata" to 40,
            "db.a.b-000001.txt" to 95_519,
            "db.a.b-000002.metadata" to 40,
            "db.a.b-000002.txt" to 97_525,
            "db.a.b-000003.metadata" to 40,
            "db.a.b-000003.txt" to 98_643,
            "db.a.b-000004.metadata" to 40,
            "db.a.b-000004.txt" to 10_913)

        outputs.forEach { outputFile ->
            val fileName = outputFile.name
            val expectedSize = expectedSizes[fileName]
            outputFile.length().toInt().also { actualSize ->
                assertEquals("File $fileName actual size $actualSize should be $expectedSize", expectedSize, actualSize)
            }
        }
    }

    @Autowired
    private lateinit var directoryWriter: DirectoryWriter

    private val outputDirectoryPath = "ephemera"

    @MockBean
    private lateinit var amazonDynamoDb: AmazonDynamoDB

    @MockBean
    private lateinit var amazonSQS: AmazonSQS
}
