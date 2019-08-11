package app.batch

import app.services.CipherService
import app.services.KeyService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.nio.file.Files
import java.nio.file.Paths

@Component
@Profile("outputToDirectory")
class DirectoryWriter(keyService: KeyService,
                      cipherService: CipherService) : Writer(keyService, cipherService) {

    override fun outputLocation(): String = outputDirectory

    override fun writeToTarget(filePath: String, fileBytes: ByteArray) {
        Files.write(Paths.get(filePath), fileBytes)
    }

    @Value("\${directory.output}")
    private lateinit var outputDirectory: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DirectoryWriter::class.toString())
    }
}