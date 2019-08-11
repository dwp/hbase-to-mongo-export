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
import java.io.OutputStream
import java.nio.file.Path


abstract class Writer<String>(private val keyService: KeyService,
                              private val cipherService: CipherService) : ItemWriter<String> {

    override fun write(items: MutableList<out String>) {
        chunkData(items)
    }

    abstract fun outputLocation(): String
    abstract fun writeOutput()
    abstract fun writeToTarget(filePath: kotlin.String, fileBytes: ByteArray)

    private fun chunkData(items: MutableList<out String>) {
        items.map { "$it\n" }.forEach { item ->
            if (batchSizeBytes + item.length > maxBatchOutputSizeBytes) {
                writeOutput()
            }
            currentBatch.append(item)
            batchSizeBytes += item.length
        }
    }

    protected fun bufferedOutputStream(outputStream: OutputStream): OutputStream =
        if (compressOutput) {
            CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2,
                BufferedOutputStream(outputStream))
        } else {
            BufferedOutputStream(outputStream)
        }

    protected fun metadataPath(number: Int): kotlin.String =
        "${outputLocation()}/$topicName-%06d.metadata".format(number)

    protected fun outputName(number: Int): kotlin.String =
        """${outputLocation()}/$topicName-%06d.txt${if (compressOutput) ".bz2" else ""}${if (encryptOutput) ".enc" else ""}"""
            .format(number)

    @Value("\${output.batch.size.max.bytes}")
    protected var maxBatchOutputSizeBytes: Int = 0

    @Value("\${compress.output:true}")
    protected var compressOutput: Boolean = true

    @Value("\${encrypt.output:true}")
    protected var encryptOutput: Boolean = true

    @Value("\${topic.name}")
    protected lateinit var topicName: kotlin.String // i.e. "db.user.data"

    protected var currentBatch = StringBuilder()
    protected var batchSizeBytes = 0
    protected var currentOutputFileNumber = 0

    companion object {
        val logger: Logger = LoggerFactory.getLogger(Writer::class.toString())
    }
}