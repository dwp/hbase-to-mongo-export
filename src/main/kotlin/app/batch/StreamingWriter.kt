package app.batch

import app.configuration.CompressionInstanceProvider
import app.domain.EncryptingOutputStream
import app.domain.ManifestRecord
import app.domain.Record
import app.services.CipherService
import app.services.KeyService
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.*
import java.security.Key
import java.security.SecureRandom
import java.security.Security
import java.util.*
import javax.crypto.spec.SecretKeySpec

@Component
@Profile("outputToS3")
class StreamingWriter(private val cipherService: CipherService,
                      private val keyService: KeyService,
                      private val secureRandom: SecureRandom,
                      private val s3: AmazonS3,
                      private val streamingManifestWriter: StreamingManifestWriter,
                      private val compressionInstanceProvider: CompressionInstanceProvider): ItemWriter<Record> {

    init {
        Security.addProvider(BouncyCastleProvider())
    }

    override fun write(items: MutableList<out Record>) {
        items.forEach { it ->
            currentBatchManifest.add(it.manifestRecord)
            val item = "${it.dbObjectAsString}\n"
            if (batchSizeBytes + item.length > maxBatchOutputSizeBytes || batchSizeBytes == 0) {
                writeOutput()
            }
            currentOutputStream!!.write(item.toByteArray())
            batchSizeBytes += item.length
            it.manifestRecord
            currentOutputStream!!.writeManifestRecord(it.manifestRecord)
        }
    }

    fun writeOutput() {
        if (batchSizeBytes > 0) {
            currentOutputStream!!.close()
            val data = currentOutputStream!!.data()

            val inputStream = ByteArrayInputStream(data)
            val bufferedInputStream = BufferedInputStream(inputStream)
            val objectKey: String = "$exportPrefix/$topicName-%06d.txt.${compressionInstanceProvider.compressionExtension()}.enc".format(currentBatch)
            val metadata = ObjectMetadata().apply {
                contentType = "binary/octetstream"
                addUserMetadata("x-amz-meta-title", objectKey)
                addUserMetadata("iv", currentOutputStream!!.initialisationVector)
                addUserMetadata("cipherText", currentOutputStream!!.dataKeyResult.ciphertextDataKey)
                addUserMetadata("dataKeyEncryptionKeyId", currentOutputStream!!.dataKeyResult.dataKeyEncryptionKeyId)
                contentLength = data.size.toLong()
            }

            logger.info("""Putting '$objectKey' size '${data.size}' into '$exportBucket', 
                        |batch size: $batchSizeBytes, max: $maxBatchOutputSizeBytes.""".trimMargin()
                    .replace("\n", ""))
            bufferedInputStream.use {
                val request = PutObjectRequest(exportBucket, objectKey, it, metadata)
                s3.putObject(request)
            }

            streamingManifestWriter.sendManifest(s3, currentOutputStream!!.manifestFile, manifestBucket, manifestPrefix)
        }
        currentOutputStream = encryptingOutputStream()
        batchSizeBytes = 0
        currentBatch++
    }


    private fun encryptingOutputStream(): EncryptingOutputStream {
        val keyResponse = keyService.batchDataKey()
        val key: Key = SecretKeySpec(Base64.getDecoder().decode(keyResponse.plaintextDataKey), "AES")
        val byteArrayOutputStream = ByteArrayOutputStream()
        val initialisationVector = ByteArray(16).apply {
            secureRandom.nextBytes(this)
        }
        val cipherOutputStream = cipherService.cipherOutputStream(key, initialisationVector, byteArrayOutputStream)
        val compressingStream = compressionInstanceProvider.compressorOutputStream(cipherOutputStream)
        val manifestFile = File("$manifestOutputDirectory/$topicName-%06d.csv".format(currentBatch))
        val manifestWriter = BufferedWriter(OutputStreamWriter(FileOutputStream(manifestFile)))

        return EncryptingOutputStream(
                BufferedOutputStream(compressingStream),
                byteArrayOutputStream,
                keyResponse,
                Base64.getEncoder().encodeToString(initialisationVector),
                manifestFile,
                manifestWriter)
    }

    private var currentOutputStream: EncryptingOutputStream? = null
    private var currentBatch = 1
    private var batchSizeBytes = 0
    private var currentBatchManifest = mutableListOf<ManifestRecord>()

    @Value("\${output.batch.size.max.bytes}")
    protected var maxBatchOutputSizeBytes: Int = 0

    @Value("\${s3.bucket}")
    private lateinit var exportBucket: String // i.e. "1234567890"

    @Value("\${s3.prefix.folder}")
    private lateinit var exportPrefix: String //i.e. "mongo-export-2019-06-23"

    @Value("\${s3.manifest.bucket}")
    private lateinit var manifestBucket: String

    @Value("\${s3.manifest.prefix.folder}")
    private lateinit var manifestPrefix: String

    @Value("\${topic.name}")
    private lateinit var topicName: String

    @Value("\${manifest.output.directory:.}")
    private lateinit var manifestOutputDirectory: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(StreamingWriter::class.toString())
    }
}
