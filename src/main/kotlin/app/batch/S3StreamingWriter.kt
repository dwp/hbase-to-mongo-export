package app.batch

import app.configuration.CompressionInstanceProvider
import app.domain.EncryptingOutputStream
import app.domain.Record
import app.services.CipherService
import app.services.ExportStatusService
import app.services.KeyService
import app.services.SnapshotSenderMessagingService
import app.utils.logging.logInfo
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.annotation.AfterStep
import org.springframework.batch.core.annotation.BeforeStep
import org.springframework.batch.core.configuration.annotation.StepScope
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
import kotlin.math.absoluteValue


@Component
@Profile("outputToS3")
@StepScope
class S3StreamingWriter(private val cipherService: CipherService,
                        private val keyService: KeyService,
                        private val secureRandom: SecureRandom,
                        private val s3: AmazonS3,
                        private val streamingManifestWriter: StreamingManifestWriter,
                        private val compressionInstanceProvider: CompressionInstanceProvider,
                        private val exportStatusService: ExportStatusService,
                        private val snapshotSenderMessagingService: SnapshotSenderMessagingService):
        ItemWriter<Record> {

    private var absoluteStart: Int = Int.MIN_VALUE
    private var absoluteStop: Int = Int.MAX_VALUE

    @BeforeStep
    fun beforeStep(stepExecution: StepExecution) {
        absoluteStart = (stepExecution.executionContext["start"] as Int).absoluteValue
        absoluteStop = (stepExecution.executionContext["stop"] as Int).absoluteValue
    }

    @AfterStep
    fun afterStep(stepExecution: StepExecution): ExitStatus {
        writeOutput(openNext = false)
        return stepExecution.exitStatus
    }


    init {
        Security.addProvider(BouncyCastleProvider())
    }

    override fun write(items: MutableList<out Record>) {
        items.forEach {
            val item = "${it.dbObjectAsString}\n"
            if (batchSizeBytes + item.length > maxBatchOutputSizeBytes || batchSizeBytes == 0) {
                writeOutput()
            }
            currentOutputStream!!.write(item.toByteArray())
            batchSizeBytes += item.length
            recordsInBatch++
            it.manifestRecord

            currentOutputStream!!.writeManifestRecord(it.manifestRecord)
        }
    }

    fun writeOutput(openNext: Boolean = true) {
        if (batchSizeBytes > 0) {
            currentOutputStream!!.close()
            val data = currentOutputStream!!.data()

            val inputStream = ByteArrayInputStream(data)
            val filePrefix = filePrefix()
            val slashRemovedPrefix = exportPrefix.replace(Regex("""/+$"""), "")
            val objectKey: String = "${slashRemovedPrefix}/$filePrefix-%06d.txt.${compressionInstanceProvider.compressionExtension()}.enc".format(currentBatch)
            val metadata = ObjectMetadata().apply {
                contentType = "binary/octetstream"
                addUserMetadata("x-amz-meta-title", objectKey)
                addUserMetadata("iv", currentOutputStream!!.initialisationVector)
                addUserMetadata("cipherText", currentOutputStream!!.dataKeyResult.ciphertextDataKey)
                addUserMetadata("dataKeyEncryptionKeyId", currentOutputStream!!.dataKeyResult.dataKeyEncryptionKeyId)
                contentLength = data.size.toLong()
            }

            logInfo(logger, "Putting batch object into bucket",
                "s3_location", objectKey, "records_in_batch", "$recordsInBatch", "batch_size_bytes", "$batchSizeBytes",
                "data_size_bytes", "${data.size}", "export_bucket", exportBucket, "max_batch_output_size_bytes", "$maxBatchOutputSizeBytes",
                "total_snapshot_files_already_written", "$totalBatches", "total_bytes_already_written", "$totalBytes",
                "total_records_already_written", "$totalRecords")

            inputStream.use {
                val request = PutObjectRequest(exportBucket, objectKey, it, metadata)
                s3.putObject(request)
            }

            logInfo(logger, "Put batch object into bucket")

            exportStatusService.incrementExportedCount()
            snapshotSenderMessagingService.notifySnapshotSender(objectKey)
            
            totalBatches++
            totalBytes += batchSizeBytes
            totalRecords += recordsInBatch

            if (streamingManifestWriter.sendManifest(s3, currentOutputStream!!.manifestFile, manifestBucket, manifestPrefix)) {
                totalManifestFiles++
                totalManifestRecords += currentOutputStream!!.manifestFile.length()
            }
        }

        if (openNext) {
            currentOutputStream = encryptingOutputStream()
            batchSizeBytes = 0
            recordsInBatch = 0
            currentBatch++
        }
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
        val filePrefix = filePrefix()
        val manifestFile = File("$manifestOutputDirectory/$filePrefix-%06d.csv".format(currentBatch))
        val manifestWriter = BufferedWriter(OutputStreamWriter(FileOutputStream(manifestFile)))

        return EncryptingOutputStream(
            BufferedOutputStream(compressingStream),
            byteArrayOutputStream,
            keyResponse,
            Base64.getEncoder().encodeToString(initialisationVector),
            manifestFile,
            manifestWriter)
    }

    private fun filePrefix() = "$topicName-%03d-%03d".format(absoluteStart, absoluteStop)

    private var currentOutputStream: EncryptingOutputStream? = null
    private var currentBatch = 0
    private var batchSizeBytes = 0
    private var recordsInBatch = 0
    private var totalBatches = 0
    private var totalBytes = 0
    private var totalRecords = 0
    private var totalManifestFiles = 0
    private var totalManifestRecords: Long = 0

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
    private lateinit var topicName: String // i.e. "db.user.data"

    @Value("\${manifest.output.directory:.}")
    private lateinit var manifestOutputDirectory: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(S3StreamingWriter::class.toString())
    }
}
