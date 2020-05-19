package app.batch

import app.utils.logging.logError
import app.utils.logging.logInfo
import com.amazonaws.services.s3.AmazonS3
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.File

@Component
class StreamingManifestWriter {

    fun sendManifest(s3: AmazonS3, manifestFile: File, manifestBucket: String, manifestPrefix: String) : Boolean {
        try {
            val manifestSize = manifestFile.length()
            val manifestFileName = manifestFile.name
            val prefix = "$manifestPrefix/$manifestFileName"

            logInfo(logger, "Writing manifest manifestFile to s3",
                "s3_location", "s3://$manifestBucket/$prefix",
                "manifest_size", "$manifestSize",
                "total_manifest_files_already_written", "$totalManifestFiles",
                "total_manifest_records_already_written", "$totalManifestRecords")

            s3.putObject(manifestBucket, prefix, manifestFile)
            totalManifestFiles++
            totalManifestRecords += manifestSize
            return true
        } catch (e: Exception) {
            logError(logger, "Failed to write manifest", e, "manifest_file", "$manifestFile")
            return false
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(StreamingManifestWriter::class.toString())
    }
    
    private var totalManifestFiles = 0
    private var totalManifestRecords : Long = 0
}
