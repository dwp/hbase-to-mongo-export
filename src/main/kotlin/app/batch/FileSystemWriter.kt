package app.batch

import app.domain.ManifestRecord
import app.services.CipherService
import app.services.KeyService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths

@Component
@Profile("outputToFile")
class FileSystemWriter(keyService: KeyService,
                       cipherService: CipherService) : Writer(keyService, cipherService){
    override fun writeManifest(manifestRecords: MutableList<ManifestRecord>) {
    }

    override fun writeToTarget(filePath: String, fileBytes: ByteArray, iv: String, cipherText: String, dataKeyEncryptionKeyId: String) {
        logger.info("writing tooooooo $filePath")
        Files.write(Paths.get(filePath), fileBytes)
    }

    override fun outputLocation(): String {
        return outputFile
    }

   /* override fun writeOutput() {
        logger.info("Appending to '$outputFile'.")
        val fw = FileWriter(outputFile, true)
        items.forEach {
            fw.write("$it\n")
        }
        fw.close()
    }*/

    @Value("\${file.output}")
    private lateinit var outputFile: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(FileSystemWriter::class.toString())
    }
}
