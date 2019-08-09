package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import app.services.CipherService
import app.services.KeyService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.*
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

@Component
@Profile("outputToDirectory")
class DirectoryWriter(private val keyService: KeyService,
                      private val cipherService: CipherService) : Writer<String>(keyService,cipherService){




    override fun writeData(encryptionResult: EncryptionResult,dataKeyResult: DataKeyResult) {
        val dataPath = outputPath(++currentOutputFileNumber)
        logger.info("Processing file number '%06d' with batchSize='$batchSizeBytes'.".format(currentOutputFileNumber))
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


    }




    private fun metadataPath(number: Int) =
            Paths.get(outputDirectory, """$tableName-%06d.metadata""".format(number))




    override fun outputPath(number: Int) = Paths.get(outputDirectory, outputName(number))

    private fun outputName(number: Int) =
            """$tableName-%06d.txt${if (compressOutput) ".bz2" else ""}${if (encryptOutput) ".enc" else ""}"""
                    .format(number)




    companion object {
        val logger: Logger = LoggerFactory.getLogger(DirectoryWriter::class.toString())
    }
}
