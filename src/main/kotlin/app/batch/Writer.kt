package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
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
import java.nio.file.Files
import java.nio.file.Path


abstract class Writer<String>(private val keyService: KeyService,
                              private val cipherService: CipherService) : ItemWriter<String> {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(Writer::class.toString())
    }

    override fun write(items: MutableList<out String>) {
        chunkData(items)
    }

    abstract fun  writeData(encryptionResult: EncryptionResult, dataKeyResult: DataKeyResult)

    abstract fun outputPath(number: Int) : Path

    private fun chunkData(items: MutableList<out String>) {
        items.map { "$it\n" }.forEach { item ->
            if (batchSizeBytes + item.length > maxBatchOutputSizeBytes) {
                writeOutput()
            }
            currentBatch.append(item)
            batchSizeBytes += item.length
        }
    }

     fun writeOutput() {
        if (batchSizeBytes > 0) {
            val dataPath = outputPath(++currentOutputFileNumber)
            val byteArrayOutputStream = ByteArrayOutputStream()

            if (encryptOutput) {
                compressIfApplicable(byteArrayOutputStream)
                encryptData(byteArrayOutputStream)
            }
            else {
                compressIfApplicable(Files.newOutputStream(dataPath)).use {
                    it.write(this.currentBatch.toString().toByteArray(StandardCharsets.UTF_8))
                }
            }

            this.currentBatch = StringBuilder()
            this.batchSizeBytes = 0
        }
    }

    private fun compressIfApplicable(outputStream: OutputStream) :OutputStream =
        if (compressOutput) {
            CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2,
                    BufferedOutputStream(outputStream))
        } else {
            BufferedOutputStream(outputStream)
        }

    private fun encryptData(byteArrayOutputStream: ByteArrayOutputStream) {
        val dataKeyResult = keyService.batchDataKey()
        logger.info("dataKeyResult: '$dataKeyResult'.")
        val encryptionResult =
                this.cipherService.encrypt(dataKeyResult.plaintextDataKey,
                        byteArrayOutputStream.toByteArray())

        writeData(encryptionResult, dataKeyResult)
    }

    @Value("\${output.batch.size.max.bytes}")
    protected var maxBatchOutputSizeBytes: Int = 0

    @Value("\${compress.output:false}")
    protected var compressOutput: Boolean = true

    @Value("\${encrypt.output:true}")
    protected var encryptOutput: Boolean = true

    protected var currentBatch = StringBuilder()
    protected var batchSizeBytes = 0

    protected var currentOutputFileNumber = 0

    @Value("\${directory.output}")
    protected lateinit var outputDirectory: kotlin.String

    @Value("\${source.table.name}")
    protected lateinit var tableName: kotlin.String // i.e. "db.user.data"

}