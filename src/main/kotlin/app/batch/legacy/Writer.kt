package app.batch.legacy

import app.domain.ManifestRecord
import app.domain.Record
import app.services.CipherService
import app.services.KeyService
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Value
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets


abstract class Writer(private val keyService: KeyService,
                      private val cipherService: CipherService) : ItemWriter<Record> {

    override fun write(items: MutableList<out Record>) {
        chunkData(items)
    }

    abstract fun outputLocation(): String
    abstract fun writeToTarget(filePath: String, fileBytes: ByteArray, iv: String, cipherText: String, dataKeyEncryptionKeyId: String)
    abstract fun writeManifest(manifestRecords: MutableList<ManifestRecord>)

    private fun chunkData(items: MutableList<out Record>) {
        items.forEach { it ->
            val item = "${it.dbObjectAsString}\n"
            if (batchSizeBytes + item.length > maxBatchOutputSizeBytes) {
                writeOutput()
                logger.info("current batch manifest ${currentBatchManifest.map { it.id }.joinToString { "," }}")
            }
            currentBatch.append(item)
            currentBatchManifest.add(it.manifestRecord)
            batchSizeBytes += item.length
        }
    }

    open fun writeOutput() {
        if (batchSizeBytes > 0) {

            val fileName = outputName(++currentOutputFileNumber)
            logger.info("Processing file $fileName with batchSizeBytes='$batchSizeBytes'.")

            try {
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

                    writeToTarget(fileName, dataBytes, encryptionResult.initialisationVector, dataKeyResult.ciphertextDataKey, dataKeyResult.dataKeyEncryptionKeyId)

                } else {
                    //no encryption
                    val byteArrayOutputStream = ByteArrayOutputStream()
                    bufferedOutputStream(byteArrayOutputStream).use {
                        it.write(this.currentBatch.toString().toByteArray(StandardCharsets.UTF_8))
                    }
                    writeToTarget(fileName, byteArrayOutputStream.toByteArray(), "", "", "")
                }

                writeManifest(currentBatchManifest)

                this.currentBatch = StringBuilder()
                this.batchSizeBytes = 0
                this.currentBatchManifest = mutableListOf()
            } catch (e: Exception) {
                logger.error("Exception while writing snapshot file '$fileName' to s3", e)
                e.printStackTrace()
            }
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
    private var currentBatchManifest = mutableListOf<ManifestRecord>()
    private var batchSizeBytes = 0
    protected var currentOutputFileNumber = 0

    companion object {
        val logger: Logger = LoggerFactory.getLogger(Writer::class.toString())
    }
}
