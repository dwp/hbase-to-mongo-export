package app.batch

import app.utils.logging.logInfo
import app.utils.logging.logError
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream

@Component
open class StreamingManifestWriter {


    fun sendManifest(s3: AmazonS3, manifestFile: File, manifestBucket: String, manifestPrefix: String) {
        try {
            val manifestSize = manifestFile.length()
            val manifestFileName = manifestFile.name
            val manifestFileMetadata = manifestMetadata(manifestFileName, manifestSize)
            val prefix = "$manifestPrefix/$manifestFileName"
            logInfo(logger, "Writing manifest '$manifestFile' to 's3://$manifestBucket/$manifestPrefix/$manifestFileName', size: $manifestSize")
            BufferedInputStream(FileInputStream(manifestFile)).use { inputStream ->
                val request = PutObjectRequest(manifestBucket, prefix, inputStream, manifestFileMetadata)
                s3.putObject(request)
            }
        } catch (e: Exception) {
            logError(logger, "Failed to write manifest: '${manifestFile}': '${e.message}'")
        }
    }

    fun manifestMetadata(fileName: String, size: Long) =
            ObjectMetadata().apply {
                contentType = "text/plain"
                addUserMetadata("x-amz-meta-title", fileName)
                contentLength = size
            }


    fun topicName(db: String, collection: String) = "db.$db.$collection"


    companion object {
        val logger: Logger = LoggerFactory.getLogger(StreamingManifestWriter::class.toString())
    }

}
