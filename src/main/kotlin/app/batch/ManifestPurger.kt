package app.batch

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.DeleteObjectRequest
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.lang.Exception

@Component
@Profile("dummyS3Client", "realS3Client")
class ManifestPurger(val s3Client: AmazonS3) {

    @Value("\${s3.bucket}")
    private lateinit var s3BucketName: String

    @Value("\${s3.manifest.prefix.folder}")
    private lateinit var s3PrefixFolder: String

    @Value("\${topic.name}")
    private lateinit var topicName: String // i.e. "db.user.data"

    fun purgeManifestFolder() {
        try {
            val objectSummaries = s3Client.listObjectsV2(s3BucketName, s3PrefixFolder).objectSummaries
            objectSummaries.forEach {
                // TODO verify if the key is the full path or just the name of the file
                val deleteObjectRequest = DeleteObjectRequest(s3BucketName, it.key)
                s3Client.deleteObject(deleteObjectRequest)
            }
        } catch (e: Exception){
            logger.error("Exception while purging manifest files",e)
        }
    }
    companion object {
        private val logger = LoggerFactory.getLogger(ManifestPurger::class.java)
    }
}





