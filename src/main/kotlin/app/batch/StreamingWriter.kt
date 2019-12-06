package app.batch

import app.configuration.CipherInstanceProvider
import app.domain.DataKeyResult
import app.domain.EncryptingOutputStream
import java.security.Security
import org.bouncycastle.jce.provider.BouncyCastleProvider
import app.domain.ManifestRecord
import app.domain.Record
import app.services.KeyService
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.*
import java.security.Key
import java.security.SecureRandom
import java.util.*
import javax.crypto.Cipher
import javax.crypto.CipherInputStream
import javax.crypto.CipherOutputStream
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

@Component
@Profile("streamingWriter")
class StreamingWriter: ItemWriter<Record> {


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
        }
    }

    fun writeOutput() {
        if (batchSizeBytes > 0) {
            currentOutputStream!!.close()
            val data = currentOutputStream!!.data()
//            val inputStream = decryptingInputStream(ByteArrayInputStream(data),
//                    currentOutputStream!!.dataKeyResult, currentOutputStream!!.initialisationVector)
//
//            inputStream.forEachLine {
//                println("WOOOOO: $it")
//            }

            val inputStream = ByteArrayInputStream(data)
            val bufferedInputStream = BufferedInputStream(inputStream)
            val objectKey: String = "$exportPrefix/$topicName-%06d.txt.bz2.enc".format(currentBatch)
            val metadata = ObjectMetadata().apply {
                contentType = "binary/octetstream"
                addUserMetadata("x-amz-meta-title", objectKey)
                addUserMetadata("iv", currentOutputStream!!.initialisationVector)
                addUserMetadata("cipherText", currentOutputStream!!.dataKeyResult.ciphertextDataKey)
                addUserMetadata("dataKeyEncryptionKeyId", currentOutputStream!!.dataKeyResult.dataKeyEncryptionKeyId)
                contentLength = data.size.toLong()
            }

            logger.info("Putting '$objectKey' size '${data.size}' into '$exportBucket'.")
            bufferedInputStream.use {
                val request = PutObjectRequest(exportBucket, objectKey, it, metadata)
                s3.putObject(request)
            }

            // TODO: write manifest to s3
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
        val cipher = encryptingCipher(key, initialisationVector)
        val cipherOutputStream = CipherOutputStream(byteArrayOutputStream, cipher)
        val compressingStream =
                CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, cipherOutputStream)
        return EncryptingOutputStream(BufferedOutputStream(compressingStream),
                byteArrayOutputStream, keyResponse,
                Base64.getEncoder().encodeToString(initialisationVector))
    }

    private fun encryptingCipher(key: Key, initialisationVector: ByteArray) =
            cipherInstanceProvider.cipherInstance().apply {
                init(Cipher.ENCRYPT_MODE, key, IvParameterSpec(initialisationVector))
            }

    private fun decryptingInputStream(inputStream: InputStream, keyResponse: DataKeyResult,
                                      initialisationVector: String): BufferedReader {
        val key: Key = SecretKeySpec(Base64.getDecoder().decode(keyResponse.plaintextDataKey), "AES")
        val cipher = decryptingCipher(key, Base64.getDecoder().decode(initialisationVector))
        val cipherInputStream = CipherInputStream(inputStream, cipher)
        val decompressingStream =
                CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.BZIP2, cipherInputStream)
        return BufferedReader(InputStreamReader(decompressingStream))
    }

    private fun decryptingCipher(key: Key, initialisationVector: ByteArray) =
            cipherInstanceProvider.cipherInstance().apply {
                init(Cipher.DECRYPT_MODE, key, IvParameterSpec(initialisationVector))
            }


    private var currentOutputStream: EncryptingOutputStream? = null
    private var currentBatch = 1
    private var batchSizeBytes = 0
    private var currentBatchManifest = mutableListOf<ManifestRecord>()

    @Autowired
    private lateinit var cipherInstanceProvider: CipherInstanceProvider

    @Autowired
    private lateinit var keyService: KeyService

    @Autowired
    private lateinit var secureRandom: SecureRandom

    @Autowired
    private lateinit var s3: AmazonS3

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

    companion object {
        val logger: Logger = LoggerFactory.getLogger(StreamingWriter::class.toString())
    }
}

