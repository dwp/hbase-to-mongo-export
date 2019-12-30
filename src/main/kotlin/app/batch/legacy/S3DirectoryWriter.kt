package app.batch.legacy

import app.domain.ManifestRecord
import app.services.CipherService
import app.services.KeyService
import app.utils.logError
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.apache.commons.text.StringEscapeUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

// See also https://github.com/aws/aws-sdk-java

@Component
@Profile("legacyOutputToS3")
class S3DirectoryWriter(keyService: KeyService,
                        cipherService: CipherService) : Writer(keyService, cipherService) {

    @Autowired
    private lateinit var s3Client: AmazonS3

    override fun writeToTarget(filePath: String, fileBytes: ByteArray, iv: String, cipherText: String, dataKeyEncryptionKeyId: String) {
        // See also https://github.com/aws/aws-sdk-java
        val bytesSize = fileBytes.size.toLong()
        logger.info("Writing snapshot to 's3://$s3BucketName/$filePath' of '$bytesSize' bytes.")

        val inputStream = ByteArrayInputStream(fileBytes)
        val bufferedInputStream = BufferedInputStream(inputStream)

        // i.e. /mongo-export-2019-06-23/db.user.data-0001.bz2.enc
        // i.e. /mongo-export-2019-06-23/db.user.data-0001.metadata
        val objKeyName: String = filePath

            // Upload a file as a new object with ContentType and title specified.
            val metadata = ObjectMetadata()
            metadata.contentType = "binary/octetstream"
            metadata.addUserMetadata("x-amz-meta-title", objKeyName)
            metadata.addUserMetadata("iv", iv)
            metadata.addUserMetadata("cipherText", cipherText)
            metadata.addUserMetadata("dataKeyEncryptionKeyId", dataKeyEncryptionKeyId)

            metadata.contentLength = bytesSize
            val request = PutObjectRequest(s3BucketName, objKeyName, bufferedInputStream, metadata)

            s3Client.putObject(request)
    }

    override fun writeManifest(manifestRecords: MutableList<ManifestRecord>) {

        try {
            val manifestFileName = generateManifestFileFormat()
            val manifestFileContent = generateEscapedCSV(manifestRecords)

            val byteArrayOutputStream = ByteArrayOutputStream()
            BufferedOutputStream(byteArrayOutputStream).use {
                it.write(manifestFileContent.toByteArray(StandardCharsets.UTF_8))
            }

            val manifestFileBytes = byteArrayOutputStream.toByteArray()
            val bytesSize = manifestFileBytes.size.toLong()
            logger.info("Writing manifest to 's3://$s3ManifestBucketName/$manifestFileName' of '$bytesSize' bytes.")

            val inputStream = ByteArrayInputStream(manifestFileBytes)
            val bufferedInputStream = BufferedInputStream(inputStream)

            val manifestFileMetadata = generateManifestFileMetadata(manifestFileName, bytesSize)

            val request = PutObjectRequest(s3ManifestBucketName, manifestFileName, bufferedInputStream, manifestFileMetadata)

            s3Client.putObject(request)
        } catch (e: Exception) {
            val joinedIds = manifestRecords.map{it.id}.joinToString(":")
            logError(logger, "Exception while writing ids to manifest files in S3", e, "ids", "${joinedIds}", "database",manifestRecords[0].db, "collection", manifestRecords[0].collection)
        }
    }

    fun generateManifestFileFormat(): String {
       return "${s3ManifestPrefixFolder}/$topicName-%06d.csv".format(currentOutputFileNumber)
    }

    fun generateEscapedCSV(manifestRecords: MutableList<ManifestRecord>): String {
        val manifestData = manifestRecords.map { "${escape(it.id)},${escape(it.timestamp.toString())},${escape(it.db)},${escape(it.collection)},${escape(it.source)},${escape(it.externalSource)}" }
        return  manifestData.joinToString("\n")
    }

    fun generateManifestFileMetadata(manifestFileName: String, bytesSize: Long): ObjectMetadata {
        val metadata = ObjectMetadata()
        metadata.contentType = "binary/octetstream"
        metadata.addUserMetadata("x-amz-meta-title", manifestFileName)
        metadata.contentLength = bytesSize
        return metadata
    }

    private fun escape(value: String): String {
       return StringEscapeUtils.escapeCsv(value)
    }

    override fun outputLocation(): String = s3PrefixFolder

    @Value("\${s3.bucket}")
    private lateinit var s3BucketName: String // i.e. "1234567890"

    @Value("\${s3.prefix.folder}")
    private lateinit var s3PrefixFolder: String //i.e. "mongo-export-2019-06-23"

    @Value("\${s3.manifest.prefix.folder}")
    private lateinit var s3ManifestPrefixFolder: String

    @Value("\${s3.manifest.bucket}")
    private lateinit var s3ManifestBucketName: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(S3DirectoryWriter::class.toString())
    }
}
