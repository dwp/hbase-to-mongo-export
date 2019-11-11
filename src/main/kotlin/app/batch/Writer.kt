package app.batch

import app.services.CipherService
import app.services.KeyService
import com.amazonaws.AmazonServiceException
import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.model.PutObjectRequest
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Value
import java.io.*
import java.nio.charset.StandardCharsets


abstract class Writer(private val keyService: KeyService,
                      private val cipherService: CipherService) : ItemWriter<String> {

    override fun write(items: MutableList<out String>) {
        chunkData(items)
    }

    abstract fun outputLocation(): String
    abstract fun writeToTarget(filePath: String, fileBytes: ByteArray, iv: String, cipherText: String, dataKeyEncryptionKeyId: String)
    abstract fun writeManifest(filePath: String, fileBytes: ByteArray)

    private fun chunkData(items: MutableList<out String>) {
        items.map { "$it\n" }.forEach { item ->
            if (batchSizeBytes + item.length > maxBatchOutputSizeBytes) {
                writeOutput()
                //writeOutputToManifest()
            }
            currentBatch.append(item)
            currentBatchManifest.append(item)
            batchSizeBytes += item.length
        }
    }

    open fun writeOutput() {
        if (batchSizeBytes > 0) {

            val dataFile = outputName(++currentOutputFileNumber)
            logger.info("Processing file $dataFile with batchSizeBytes='$batchSizeBytes'.")

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

                val dataBytes = encryptionResult.encrypted.toByteArray(StandardCharsets.US_ASCII)

                writeToTarget(dataFile, dataBytes, encryptionResult.initialisationVector, dataKeyResult.ciphertextDataKey, dataKeyResult.dataKeyEncryptionKeyId)

            } else {
                //no encryption
                val byteArrayOutputStream = ByteArrayOutputStream()
                bufferedOutputStream(byteArrayOutputStream).use {
                    it.write(this.currentBatch.toString().toByteArray(StandardCharsets.UTF_8))
                }
                writeToTarget(dataFile, byteArrayOutputStream.toByteArray(), "", "", "")
            }

            this.currentBatch = StringBuilder()
            this.batchSizeBytes = 0
        }
    }

    fun writeOutputToManifest() {

        if (batchSizeBytes > 0) {

            val dataFile = outputName(++currentOutputFileNumber)
            logger.info("Processing file $dataFile with batchSizeBytes='$batchSizeBytes'.")

            if (encryptOutput) {
                val byteArrayOutputStream = ByteArrayOutputStream()

                bufferedOutputStream(byteArrayOutputStream).use {
                    it.write(this.currentBatchManifest.toString().toByteArray(StandardCharsets.UTF_8))
                }

                val dataBytes = byteArrayOutputStream.toByteArray()

               writeManifest(dataFile, dataBytes)

            }

            this.currentBatchManifest = StringBuilder()
        }
    }

    private fun bufferedOutputStream(outputStream: OutputStream): OutputStream =
        if (compressOutput) {
            CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2,
                BufferedOutputStream(outputStream))
        } else {
            BufferedOutputStream(outputStream)
        }

    protected fun metadataPath(number: Int): String =
        "${outputLocation()}/$topicName-%06d.metadata".format(number)

    private fun outputName(number: Int): String =
        """${outputLocation()}/$topicName-%06d.txt${if (compressOutput) ".bz2" else ""}${if (encryptOutput) ".enc" else ""}"""
            .format(number)

    @Value("\${output.batch.size.max.bytes}")
    protected var maxBatchOutputSizeBytes: Int = 0

    @Value("\${compress.output:true}")
    protected var compressOutput: Boolean = true

    @Value("\${encrypt.output:true}")
    protected var encryptOutput: Boolean = true

    @Value("\${topic.name}")
    protected lateinit var topicName: String // i.e. "db.user.data"

    private var currentBatch = StringBuilder()
    private var currentBatchManifest = StringBuilder()
    private var batchSizeBytes = 0
    protected var currentOutputFileNumber = 0

    companion object {
        val logger: Logger = LoggerFactory.getLogger(Writer::class.toString())
    }
}
