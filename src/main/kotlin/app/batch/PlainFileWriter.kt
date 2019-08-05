package app.batch

import app.services.CipherService
import app.services.KeyService
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.*
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

@Component
@Profile("outputToFile")
class FileSystemWriter : ItemWriter<String> {

    override fun write(items: MutableList<out String>) {
        logger.info("Appending to '$outputFile'.")
        val fw = FileWriter(outputFile, true)
        items.forEach {
            fw.write("$it\n")
        }
        fw.close()
    }

    @Value("\${file.output}")
    private lateinit var outputFile: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(FileSystemWriter::class.toString())
    }
}