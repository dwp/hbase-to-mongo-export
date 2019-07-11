package app.batch

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.File
import java.io.FileWriter
import java.io.Writer

@Component
@Profile("outputToDirectory")
class DirectoryWriter: ItemWriter<String> {

    override fun write(items: MutableList<out String>) {
        this.currentOutput = currentOutput()
        if (this.currentOutput != null) {
            items.forEach {
                if (dataWrittenToCurrentOutput + it.length > maxFileSize) {
                    currentOutput?.close()
                    currentOutput = nextOutput()
                    dataWrittenToCurrentOutput = 0
                }
                currentOutput?.write("$it\n")
                dataWrittenToCurrentOutput += it.length
            }
            currentOutput?.flush()
        }
    }

    private fun currentOutput() =
            if (currentOutput != null) currentOutput
            else FileWriter(currentOutputFile())

    private fun nextOutput() = FileWriter(outputFile(++currentOutputFileNumber))
    private fun currentOutputFile() = outputFile(currentOutputFileNumber)
    private fun outputFile(number: Int) = File(outputDirectory, "${tableName}-${number}.txt")

    private var currentOutput: Writer? = null
    private var currentOutputFileNumber = 1
    private var dataWrittenToCurrentOutput = 0

    @Value("\${hbase.crown.export.directory.output}")
    private lateinit var outputDirectory: String

    @Value("\${hbase.crown.export.file.size.max}")
    private var maxFileSize: Int = 0

    @Value("\${source.table.name}")
    private lateinit var tableName: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DirectoryWriter::class.toString())
    }
}