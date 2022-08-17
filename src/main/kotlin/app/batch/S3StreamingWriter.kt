package app.batch

import app.configuration.CompressionInstanceProvider
import app.domain.EncryptingOutputStream
import app.domain.Record
import app.exceptions.DataKeyServiceUnavailableException
import app.services.*
import com.amazonaws.services.s3.AmazonS3
import io.prometheus.client.Counter
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.annotation.AfterStep
import org.springframework.batch.core.annotation.BeforeStep
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger
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
                        private val messagingService: MessagingService,
                        private val s3ObjectService: S3ObjectService,
                        private val recordCounter: Counter,
                        private val byteCounter: Counter,
                        private val failedBatchPutCounter: Counter,
                        private val failedManifestPutCounter: Counter,
                        private val dksNewDataKeyFailuresCounter: Counter):

    ItemWriter<Record> {

    private var absoluteStart: Int = Int.MIN_VALUE
    private var absoluteStop: Int = Int.MAX_VALUE
    private var start: Int = Int.MIN_VALUE
    private var stop: Int = Int.MAX_VALUE

    @BeforeStep
    fun beforeStep(stepExecution: StepExecution) {
        absoluteStart = (stepExecution.executionContext["start"] as Int).absoluteValue
        absoluteStop = (stepExecution.executionContext["stop"] as Int).absoluteValue
        start = stepExecution.executionContext["start"] as Int
        stop = stepExecution.executionContext["stop"] as Int
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
        try {
            items.forEach { record ->
                val item = "${record.dbObjectAsString}\n"

                if (batchSizeBytes + item.length > maxBatchOutputSizeBytes || batchSizeBytes == 0) {
                    writeOutput()
                }

                currentOutputStream?.let { output ->
                    output.write(item.toByteArray())
                    batchSizeBytes += item.length
                    recordsInBatch++
                    output.writeManifestRecord(record.manifestRecord)
                }
            }
        } catch (e: Exception) {
            logger.error("Error in write", e)
            throw e
        }
    }

    fun writeOutput(openNext: Boolean = true) {
        if (batchSizeBytes > 0) {
            val filePrefix = filePrefix()
            val slashRemovedPrefix = exportPrefix.replace(Regex("""/+$"""), "")
            val objectKey =
                "${slashRemovedPrefix}/$filePrefix-%06d.txt.${compressionInstanceProvider.compressionExtension()}.enc"
                    .format(currentBatch)

            currentOutputStream?.let { encryptingOutputStream ->
                try {
                    val closed = encryptingOutputStream.close()
                    val data = encryptingOutputStream.data()

                    if (!closed) {
                        logger.error("Failed to close output streams cleanly", "object_key" to objectKey)
                    }

                    try {
                        s3ObjectService.putObject(objectKey, encryptingOutputStream)
                    } catch (e: Exception) {
                        failedBatchPutCounter.labels(split()).inc()
                        throw e
                    }

                    logger.info("Put batch object into bucket",
                        "s3_location" to objectKey,
                        "records_in_batch" to "$recordsInBatch",
                        "batch_size_bytes" to "$batchSizeBytes",
                        "data_size_bytes" to "${data.size}",
                        "export_bucket" to exportBucket,
                        "max_batch_output_size_bytes" to "$maxBatchOutputSizeBytes",
                        "total_snapshot_files_already_written" to "$totalBatches",
                        "total_bytes_already_written" to "$totalBytes",
                        "topic_name" to "$topicName",
                        "total_records_already_written" to "$totalRecords")

                    exportStatusService.incrementExportedCount(objectKey)
                    messagingService.notifySnapshotSender(objectKey)
                    totalBatches++
                    totalBytes += batchSizeBytes
                    totalRecords += recordsInBatch

                    recordCounter.labels(split()).inc(recordsInBatch.toDouble())
                    byteCounter.labels(split()).inc(batchSizeBytes.toDouble())

                    try {
                        if (snapshotType == "incremental") {
                            streamingManifestWriter.sendManifest(s3, encryptingOutputStream.manifestFile, manifestBucket, manifestPrefix)
                        }
                    } catch (e: Exception) {
                        failedManifestPutCounter.labels(split()).inc()
                        throw e
                    }

                    totalManifestFiles++
                    totalManifestRecords += encryptingOutputStream.manifestFile.length()
                } catch (e: Exception) {
                    logger.error("Failed to write data", e,"object_key" to objectKey, "manifest_file" to "${encryptingOutputStream.manifestFile}")
                }
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
        try {
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

            return EncryptingOutputStream(BufferedOutputStream(compressingStream),
                                            byteArrayOutputStream,
                                            keyResponse,
                                            Base64.getEncoder().encodeToString(initialisationVector),
                                            manifestFile,
                                            manifestWriter)
        } catch (e: DataKeyServiceUnavailableException) {
            dksNewDataKeyFailuresCounter.inc()
            throw e
        }
    }

    // private fun filePrefix() = "$topicName-%03d-%03d".format(absoluteStart, absoluteStop)
    private fun filePrefix(): String {
        var renderedTopicName = if (topicName.count{ c -> c == '.' } == 2) {
            if (topicName.lastIndexOf('-') > topicName.lastIndexOf('.')) {
                topicName.substring(0, topicName.lastIndexOf('-')).plus(
                    topicName.subSequence(topicName.lastIndexOf('-') + 1, topicName.length)[0].toUpperCase()
                ).plus(topicName.subSequence(topicName.lastIndexOf('-') + 1, topicName.length).substring(1))
            } else topicName
        } else topicName
        return "$renderedTopicName-%03d-%03d".format(absoluteStart, absoluteStop)
    }

    private fun split() = "%03d-%03d".format(absoluteStart, absoluteStop)

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

    @Value("\${snapshot.type}")
    private lateinit var snapshotType: String

    companion object {
        val logger = DataworksLogger.getLogger(S3StreamingWriter::class)
    }
}
