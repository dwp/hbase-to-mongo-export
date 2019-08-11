package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import app.services.CipherService
import app.services.KeyService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.FileWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Path

@Component
@Profile("outputToFile")
class FileSystemWriter(keyService: KeyService,
                       cipherService: CipherService) : Writer<String>(keyService, cipherService) {

    override fun outputPath(number: Int): Path {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Value("\${file.output}")
    private lateinit var outputFile: String

    override fun writeData(encryptionResult: EncryptionResult, dataKeyResult: DataKeyResult) {
        logger.info("Appending to '$outputFile'.")
        val fw = FileWriter(outputFile, true)
        fw.write(String(encryptionResult.encrypted.toByteArray(StandardCharsets.US_ASCII), Charsets.UTF_8).toCharArray())
        fw.close()
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(FileSystemWriter::class.toString())
    }
}