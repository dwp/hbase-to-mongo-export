package app.batch.legacy

import app.domain.ManifestRecord
import app.services.CipherService
import app.services.KeyService
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.nio.file.Files
import java.nio.file.Paths

@Component
@Profile("outputToFile")
class FileSystemWriter(keyService: KeyService,
                       cipherService: CipherService) : Writer(keyService, cipherService) {
    override fun writeManifest(manifestRecords: MutableList<ManifestRecord>) {
    }

    override fun writeToTarget(filePath: String, fileBytes: ByteArray, iv: String, cipherText: String, dataKeyEncryptionKeyId: String) {
        logger.info("Writing to file", "file_name" to filePath)
        Files.write(Paths.get(filePath), fileBytes)
    }

    override fun outputLocation(): String {
        return outputFile
    }

    @Value("\${file.output}")
    private lateinit var outputFile: String

    companion object {
        val logger = DataworksLogger.getLogger(FileSystemWriter::class.toString())
    }
}
