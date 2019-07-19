package app.batch

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory
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
@ActiveProfiles("phoneyDataKeyService", "phoneyDecryptionService", "unitTest", "outputToDirectory")
@SpringBootTest
@TestPropertySource(properties = [
    "directory.output=ephemera",
    "file.size.max=100000",
    "source.table.name=ucdata",
    "compress.output=true"
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
        val listOfLists: MutableList<MutableList<String>> = mutableListOf()
        var total = 0

        for (i in 1 .. 10) {
            val list: MutableList<String> = mutableListOf()
            for (j in 1 .. 10) {
                val token = "[%03d/%04d]".format(i, j)
                val item = token.repeat(j * (11 - i) * 10)
                list.add(item)
                total += item.length
            }
            listOfLists.add(list)
        }

        System.setProperty("compress.output", "true")
        listOfLists.forEach {
            directoryWriter.write(it)
        }

        directoryWriter.closeOutput()
        val outputs = File(outputDirectoryPath).list()
        assertEquals(4, outputs.size)
        val filenameRegex = Regex("""(\d+)\.\w+\.bz2$""")

        outputs.forEach {
            logger.info("Checking $it.")
            val match = filenameRegex.find(it)

            match?.groups?.get(1)?.value?.toInt().let {
                _ ->
                    val fin = Files.newInputStream(Paths.get("ephemera", it))
                    val buffered = BufferedInputStream(fin)
                    val bzIn = BZip2CompressorInputStream(buffered)
                    val buffer = ByteArray(1000)
                    var n: Int

                    do {
                        n = bzIn.read(buffer)
                    }
                    while (n  != -1)

                    bzIn.close()
            }
        }


    }

    @Autowired
    private lateinit var directoryWriter: DirectoryWriter

    private val outputDirectoryPath = "ephemera"

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DirectoryWriterCompressionTest::class.toString())
    }

}