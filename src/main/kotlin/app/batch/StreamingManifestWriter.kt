package app.batch

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.File
import java.io.FileInputStream

@Component
class StreamingManifestWriter {

    @Retryable(value = [Exception::class],
            maxAttemptsExpression = "\${manifest.retry.maxAttempts:5}",
            backoff = Backoff(delayExpression = "\${manifest.retry.delay:1000}",
                              multiplierExpression = "\${manifest.retry.multiplier:2}"))
    fun sendManifest(s3: AmazonS3, manifestFile: File, manifestBucket: String, manifestPrefix: String) {
        val manifestSize = manifestFile.length()
        val manifestFileName = manifestFile.name
        val manifestFileMetadata = manifestMetadata(manifestFileName, manifestSize)
        val prefix = "$manifestPrefix/$manifestFileName"

        logger.info("Writing manifest to s3",
                "s3_location" to "s3://$manifestBucket/$prefix",
                "manifest_size" to "$manifestSize",
                "total_manifest_files_already_written" to "$totalManifestFiles",
                "total_manifest_records_already_written" to "$totalManifestRecords")

        try {
            FileInputStream(manifestFile).use { inputStream ->
                val request = PutObjectRequest(manifestBucket, prefix, inputStream, manifestFileMetadata)
                s3.putObject(request)
            }
        } catch (e: Exception) {
            logger.error("Failed to write manifest to s3", e,
                "s3_location" to "s3://$manifestBucket/$prefix",
                "manifest_size" to "$manifestSize",
                "total_manifest_files_already_written" to "$totalManifestFiles",
                "total_manifest_records_already_written" to "$totalManifestRecords")
            throw e
        }

        logger.info("Written manifest to s3",
            "s3_location" to "s3://$manifestBucket/$prefix",
            "manifest_size" to "$manifestSize",
            "total_manifest_files_already_written" to "$totalManifestFiles",
            "total_manifest_records_already_written" to "$totalManifestRecords")

        totalManifestFiles++
        totalManifestRecords += manifestSize
    }

    fun manifestMetadata(fileName: String, size: Long) =
            ObjectMetadata().apply {
                contentType = "text/plain"
                addUserMetadata("x-amz-meta-title", fileName)
                contentLength = size
            }


    companion object {
        val logger = DataworksLogger.getLogger(StreamingManifestWriter::class)
    }


    private var totalManifestFiles = 0
    private var totalManifestRecords : Long = 0
}
