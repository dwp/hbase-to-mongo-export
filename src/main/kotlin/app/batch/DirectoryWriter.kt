package app.batch

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
class DirectoryWriter(private val keyService: KeyService,
                      private val cipherService: CipherService) : Writer<String>(keyService, cipherService) {

    override fun writeOutput() {
        if (batchSizeBytes > 0) {

            val dataPath = outputName(++currentOutputFileNumber)
            logger.info("Processing file $dataPath with batchSizeBytes='$batchSizeBytes'.".format(dataPath))

            if (encryptOutput) {
                val dataKeyResult = keyService.batchDataKey()
                logger.info("dataKeyResult: '$dataKeyResult'.")
                val byteArrayOutputStream = ByteArrayOutputStream()

                bufferedOutputStream(byteArrayOutputStream).use {
                    it.write(this.currentBatch.toString().toByteArray(StandardCharsets.UTF_8))
                }

                val encryptionResult =
                    this.cipherService.encrypt(dataKeyResult.plaintextDataKey,
                        byteArrayOutputStream.toByteArray())

                writeToTarget(dataPath,
                    encryptionResult.encrypted.toByteArray(StandardCharsets.US_ASCII))
                logger.info("Wrote dataPath: '$dataPath'.")

                val metadataFile = metadataPath(currentOutputFileNumber)
                val metadataByteArrayOutputStream = ByteArrayOutputStream()
                val metadataStream: OutputStream = BufferedOutputStream(metadataByteArrayOutputStream)
                metadataStream.use {
                    val iv = encryptionResult.initialisationVector
                    it.write("iv=$iv\n".toByteArray(StandardCharsets.UTF_8))
                    it.write("ciphertext=${dataKeyResult.ciphertextDataKey}\n".toByteArray(StandardCharsets.UTF_8))
                    it.write("dataKeyEncryptionKeyId=${dataKeyResult.dataKeyEncryptionKeyId}\n".toByteArray(StandardCharsets.UTF_8))
                }
                val metadataBytes = metadataByteArrayOutputStream.toByteArray()
                writeToTarget(metadataFile, metadataBytes)
                logger.info("Wrote metadataPath: '$metadataFile'.")
            } else {
                val byteArrayOutputStream = ByteArrayOutputStream()
                bufferedOutputStream(byteArrayOutputStream).use {
                    it.write(this.currentBatch.toString().toByteArray(StandardCharsets.UTF_8))
                }
                writeToTarget(dataPath, byteArrayOutputStream.toByteArray())
                logger.info("Wrote dataPath: '$dataPath'.")
            }

            this.currentBatch = StringBuilder()
            this.batchSizeBytes = 0
        }
    }

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