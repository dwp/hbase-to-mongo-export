package app.batch

import app.services.CipherService
import app.services.KeyService
import com.amazonaws.AmazonServiceException
import com.amazonaws.SdkClientException
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.BufferedInputStream
import java.io.ByteArrayInputStream

// See also https://github.com/aws/aws-sdk-java

@Component
@Profile("outputToS3")
class S3DirectoryWriter(keyService: KeyService,
                        cipherService: CipherService) : Writer(keyService, cipherService) {

    @Autowired
    private  lateinit var s3Client: AmazonS3Client

    override fun writeToTarget(filePath: String, fileBytes: ByteArray) {
        // See also https://github.com/aws/aws-sdk-java
        val bytesSize = fileBytes.size.toLong()
        logger.info("Writing file 's3://$s3BucketName/$filePath' of '$bytesSize' bytes.")

        val inputStream = ByteArrayInputStream(fileBytes)
        val bufferedInputStream = BufferedInputStream(inputStream)

        // eu-west-1 -> EU_WEST_2 (i.e tf style to enum name)
        val updatedRegion = region.toUpperCase().replace("-", "_")
        val clientRegion = Regions.valueOf(updatedRegion)

        // i.e. /mongo-export-2019-06-23/db.user.data-0001.bz2.enc
        // i.e. /mongo-export-2019-06-23/db.user.data-0001.metadata
        val objKeyName: String = filePath

        try {
            // Upload a file as a new object with ContentType and title specified.
            val metadata = ObjectMetadata()
            metadata.contentType = "binary/octetstream"
            metadata.addUserMetadata("x-amz-meta-title", objKeyName)
            metadata.contentLength = bytesSize
            val request = PutObjectRequest(s3BucketName, objKeyName, bufferedInputStream, metadata)

            s3Client.putObject(request)
        } catch (e: AmazonServiceException) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace()
        } catch (e: SdkClientException) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace()
        }
    }

    override fun outputLocation(): String = s3PrefixFolder

    @Value("\${aws.region}")
    private var region: String = "eu-west-2"

    @Value("\${s3.bucket}")
    private lateinit var s3BucketName: String // i.e. "1234567890"

    @Value("\${s3.prefix.folder}")
    private lateinit var s3PrefixFolder: String //i.e. "mongo-export-2019-06-23"

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DirectoryWriter::class.toString())
    }
}