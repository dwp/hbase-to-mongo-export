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
import java.nio.file.Path
import java.nio.file.Paths

@Component
@Profile("outputToDirectory")
class DirectoryWriter(private val keyService: KeyService,
                      private val cipherService: CipherService) : Writer<String>(keyService, cipherService) {

    override fun writeOutput() {
        if (batchSizeBytes > 0) {

            val dataPath = outputPath(++currentOutputFileNumber)
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

                Files.write(dataPath,
                    encryptionResult.encrypted.toByteArray(StandardCharsets.US_ASCII))
                logger.info("Wrote dataPath: '$dataPath'.")

                val metadataPath = metadataPath(currentOutputFileNumber)
                BufferedWriter(OutputStreamWriter(Files.newOutputStream(metadataPath))).use {
                    val iv = encryptionResult.initialisationVector
                    //val plaintext = dataKeyResult.plaintextDataKey //TODO ask Dan C about this
                    it.write("iv=$iv\n")
                    it.write("ciphertext=${dataKeyResult.ciphertextDataKey}\n")
                    it.write("dataKeyEncryptionKeyId=${dataKeyResult.dataKeyEncryptionKeyId}\n")
                }
                logger.info("Wrote metadataPath: '$metadataPath'.")
            } else {
                bufferedOutputStream(Files.newOutputStream(dataPath)).use {
                    it.write(this.currentBatch.toString().toByteArray(StandardCharsets.UTF_8))
                }
                logger.info("Wrote dataPath: '$dataPath'.")
            }

            this.currentBatch = StringBuilder()
            this.batchSizeBytes = 0
        }
    }


    private fun metadataPath(number: Int) =
        Paths.get(outputDirectory, """$topicName-%06d.metadata""".format(number))

    override fun outputPath(number: Int): Path = Paths.get(outputDirectory, outputName(number))

    private fun outputName(number: Int) =
        """$topicName-%06d.txt${if (compressOutput) ".bz2" else ""}${if (encryptOutput) ".enc" else ""}"""
            .format(number)

    @Value("\${directory.output}")
    private lateinit var outputDirectory: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DirectoryWriter::class.toString())
    }
}