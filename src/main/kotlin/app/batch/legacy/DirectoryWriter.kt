package app.batch.legacy

import app.domain.ManifestRecord
import app.services.CipherService
import app.services.KeyService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

@Component
@Profile("outputToDirectory")
class DirectoryWriter(keyService: KeyService,
                      cipherService: CipherService) : Writer(keyService, cipherService) {
    override fun writeManifest(manifestRecords: MutableList<ManifestRecord>) {

    }

    override fun outputLocation(): String = outputDirectory

    override fun writeToTarget(filePath: String, fileBytes: ByteArray, iv: String, cipherText: String, dataKeyEncryptionKeyId: String) {
        Files.write(Paths.get(filePath), fileBytes)

        val metadataFile = metadataPath(currentOutputFileNumber)
        val metadataByteArrayOutputStream = ByteArrayOutputStream()
        val metadataStream: OutputStream = BufferedOutputStream(metadataByteArrayOutputStream)
        metadataStream.use {
            it.write("iv=$iv\n".toByteArray(StandardCharsets.UTF_8))
            it.write("ciphertext=$cipherText\n".toByteArray(StandardCharsets.UTF_8))
            it.write("dataKeyEncryptionKeyId=$dataKeyEncryptionKeyId\n".toByteArray(StandardCharsets.UTF_8))
        }
        val metadataBytes = metadataByteArrayOutputStream.toByteArray()
        Files.write(Paths.get(metadataFile), metadataBytes)
    }

    @Value("\${directory.output}")
    private lateinit var outputDirectory: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DirectoryWriter::class.toString())
    }
}
