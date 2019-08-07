package app.batch

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
import java.io.File

@RunWith(SpringRunner::class)
@ActiveProfiles("phoneyDataKeyService", "phoneyCipherService", "unitTest", "outputToDirectory")
@SpringBootTest
@TestPropertySource(properties = [
    "directory.output=ephemera",
    "output.batch.size.max.bytes=100000",
    "source.table.name=ucdata",
    "compress.output=false",
    "encrypt.output=false"
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

        assertEquals(4, outputs.size)

        val expectedSizes = mapOf(1 to 95_519, 2 to 97_525, 3 to 98_643, 4 to 10_913)

        val filenameRegex = Regex("""(\d+)\.\w+$""")

        outputs.forEach { outputFile ->
            logger.info("Checking $outputFile.")
            val match = filenameRegex.find(outputFile.name)

            match?.groups?.get(1)?.value?.toInt().let {
                fileNumber ->
                    val expectedSize = expectedSizes[fileNumber]
                    outputFile.length().toInt().also {actualSize ->
                        logger.info("Checking that $outputFile actual size $actualSize is the expected size $expectedSize")
                        assertEquals(expectedSize, actualSize)
                    }
            }

        }

    }

    @Autowired
    private lateinit var directoryWriter: DirectoryWriter

    private val outputDirectoryPath = "ephemera"

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DirectoryWriterChunkingTest::class.toString())
    }

}