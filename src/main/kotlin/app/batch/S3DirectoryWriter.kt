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

// See https://github.com/aws/aws-sdk-java-v2
// See https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/java/example_code/s3/src/main/java/CopyObjectSingleOperation.java
@Component
@Profile("outputToS3Directory")
class S3DirectoryWriter(private val keyService: KeyService,
                        private val cipherService: CipherService) : ItemWriter<String> {

    override fun write(items: MutableList<out String>) {
        items.map { "$it\n" }.forEach { item ->
            if (batchSize + item.length > maxBatchOutputSize) {
                writeOutput()
            }
            currentBatch.append(item)
            batchSize += item.length
        }
    }

    fun writeOutput() {
        if (batchSize > 0) {
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

                Files.write(outputPath(++currentOutputFileNumber),
                        encryptionResult.encrypted.toByteArray(StandardCharsets.US_ASCII))

                val metadataFile = metadataPath()
                val metadataByteArrayOutputStream = ByteArrayOutputStream()
                val metadataStream: OutputStream = BufferedOutputStream(metadataByteArrayOutputStream)
                metadataStream.use {
                    val iv = encryptionResult.initialisationVector
                    val plaintext = dataKeyResult.plaintextDataKey
                    it.write("iv=$iv\n".toByteArray(StandardCharsets.UTF_8))
                    it.write("ciphertext=${dataKeyResult.ciphertextDataKey}\n".toByteArray(StandardCharsets.UTF_8))
                    it.write("dataKeyEncryptionKeyId=${dataKeyResult.dataKeyEncryptionKeyId}\n".toByteArray(StandardCharsets.UTF_8))
                }
                Files.write(metadataFile, metadataByteArrayOutputStream.toByteArray())

            } else {
                bufferedOutputStream(Files.newOutputStream(outputPath(++currentOutputFileNumber))).use {
                    it.write(this.currentBatch.toString().toByteArray(StandardCharsets.UTF_8))
                }
            }

            this.currentBatch = StringBuilder()
            this.batchSize = 0
        }
    }


    private fun bufferedOutputStream(outputStream: OutputStream): OutputStream =
            if (compressOutput) {
                CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2,
                        BufferedOutputStream(outputStream))
            } else {
                BufferedOutputStream(outputStream)
            }

    private fun metadataPath() =
            Paths.get(s3outputDirectory, """$tableName-%04d.metadata""".format(currentOutputFileNumber))


    private var currentBatch = StringBuilder()
    private var batchSize = 0

    private fun outputPath(number: Int) = Paths.get(s3outputDirectory, outputName(number))

    private fun outputName(number: Int) =
            """$tableName-%04d.txt${if (compressOutput) ".bz2" else ""}${if (encryptOutput) ".enc" else ""}"""
                    .format(number)


    private var currentOutputFileNumber = 0

    @Value("\${output.batch.size.max}")
    private var maxBatchOutputSize: Int = 0

    @Value("\${s3folder.output}")
    private lateinit var s3outputDirectory: String

    @Value("\${source.table.name}")
    private lateinit var tableName: String

    @Value("\${compress.output:false}")
    private var compressOutput: Boolean = true

    @Value("\${encrypt.output:true}")
    private var encryptOutput: Boolean = true

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DirectoryWriter::class.toString())
    }
}
