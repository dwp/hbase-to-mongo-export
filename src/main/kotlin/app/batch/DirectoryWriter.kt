package app.batch

import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.BufferedOutputStream
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.io.Writer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

@Component
@Profile("outputToDirectory")
class DirectoryWriter: ItemWriter<String> {

    override fun write(items: MutableList<out String>) {
        this.currentOutput = currentWriter()
        items.forEach {
            if (dataWrittenToCurrentOutput + it.length + 1 > maxFileSize) {
                if (dataWrittenToCurrentOutput > 0) {
                    this.currentOutput?.close()
                    this.currentOutput = nextOutput()
                    this.dataWrittenToCurrentOutput = 0
                }
            }
            currentOutput?.write("$it\n")
            dataWrittenToCurrentOutput += (it.length + 1)
        }
        this.currentOutput?.flush()
    }

    private fun currentWriter() =
        if (currentOutput != null) {
            currentOutput
        }
        else {
            writer(BufferedOutputStream(Files.newOutputStream((outputPath(currentOutputFileNumber)))))
        }

    private fun nextOutput() = writer(BufferedOutputStream(Files.newOutputStream(outputPath(++currentOutputFileNumber))))

    private fun writer(buffered: OutputStream): OutputStreamWriter =
        OutputStreamWriter(
                if (compressOutput) {
                    CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, buffered)
                }
                else {
                    buffered
                },
                StandardCharsets.UTF_8)

    fun closeOutput() {
        this.currentWriter()?.close()
    }

    private fun outputPath(number: Int) =
            Paths.get(outputDirectory, """$tableName-%04d.txt${if (compressOutput) ".bz2" else ""}""".format(number))

    private var currentOutput: Writer? = null
    private var currentOutputFileNumber = 1
    private var dataWrittenToCurrentOutput = 0

    @Value("\${directory.output}")
    private lateinit var outputDirectory: String

    @Value("\${file.size.max}")
    private var maxFileSize: Int = 0

    @Value("\${source.table.name}")
    private lateinit var tableName: String

    @Value("\${compress.output:false}")
    private var compressOutput: Boolean = false

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DirectoryWriter::class.toString())
    }
}