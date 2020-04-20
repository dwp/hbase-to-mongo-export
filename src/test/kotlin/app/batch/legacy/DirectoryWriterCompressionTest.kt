package app.batch.legacy

import app.domain.ManifestRecord
import app.domain.Record
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.BufferedInputStream
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths


@RunWith(SpringRunner::class)
@ActiveProfiles("phoneyDataKeyService", "phoneyCipherService", "unitTest", "outputToDirectory")
@SpringBootTest
@TestPropertySource(properties = [
    "directory.output=ephemera",
    "output.batch.size.max.bytes=100000",
    "data.table.name=ucfs-data",
    "compress.output=true",
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
    "aws.region=eu-west-2"
])
class DirectoryWriterCompressionTest {

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

    /*
     * Checks output files are valid bzip2 - if they can be decompressed without
     * error they are deemed to be valid.
     */
    @Test
    fun testCompression() {
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

        System.setProperty("compress.output", "true")
        listOfLists.forEach {
            directoryWriter.write(it)
        }

        directoryWriter.writeOutput()
        val outputs = File(outputDirectoryPath).list()
        assertEquals(8, outputs.size)
        val filenameRegex = Regex("""(\d+)\.\w+\.txt.bz2$""")

        outputs.forEach {
            if (it.endsWith(".txt.bz2")) {
                val match = filenameRegex.find(it)

                match?.groups?.get(1)?.value?.toInt().let { _ ->
                    val fin = Files.newInputStream(Paths.get("ephemera", it))
                    val buffered = BufferedInputStream(fin)
                    val bzIn = BZip2CompressorInputStream(buffered)
                    val buffer = ByteArray(1000)
                    var n: Int

                    do {
                        n = bzIn.read(buffer)
                    } while (n != -1)

                    bzIn.close()
                }
            }
        }


    }

    @Autowired
    private lateinit var directoryWriter: DirectoryWriter

    private val outputDirectoryPath = "ephemera"

}
