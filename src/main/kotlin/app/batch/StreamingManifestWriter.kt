package app.batch

import app.utils.logging.logInfo
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component
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

        logInfo(logger, "Writing manifest manifestFile to s3",
                "s3_location", "s3://$manifestBucket/$prefix",
                "manifest_size", "$manifestSize",
                "total_manifest_files_already_written", "$totalManifestFiles",
                "total_manifest_records_already_written", "$totalManifestRecords")

        FileInputStream(manifestFile).use { inputStream ->
            val request = PutObjectRequest(manifestBucket, prefix, inputStream, manifestFileMetadata)
            s3.putObject(request)
        }

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
        val logger: Logger = LoggerFactory.getLogger(StreamingManifestWriter::class.toString())
    }


    private var totalManifestFiles = 0
    private var totalManifestRecords : Long = 0
}
