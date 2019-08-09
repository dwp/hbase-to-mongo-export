package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import app.services.CipherService
import app.services.KeyService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.*

import com.amazonaws.AmazonServiceException
import com.amazonaws.SdkClientException
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths

// See also https://github.com/aws/aws-sdk-java

@Component
@Profile("outputToS3")
class S3DirectoryWriter(private val keyService: KeyService,
                        private val cipherService: CipherService) : Writer<String>(keyService,cipherService){

    @Value("\${aws.region}")
    private var region: kotlin.String = "eu-west-1"

    @Value("\${s3.bucket}")
    private lateinit var s3BucketName: kotlin.String // i.e. "1234567890"

    @Value("\${s3.prefix.folder}")
    private lateinit var s3PrefixFolder: kotlin.String //i.e. "mongo-export-2019-06-23"

    val updated_region = region.toUpperCase().replace("-", "_")
    val clientRegion = Regions.valueOf(updated_region)


    val s3Client = AmazonS3ClientBuilder.standard()
            .withRegion(clientRegion)
            .build()


    override fun writeData( encryptionResult: EncryptionResult, dataKeyResult: DataKeyResult) {
        // See also https://github.com/aws/aws-sdk-java
        val fullFilePath = outputPath(++currentOutputFileNumber)
        val fileBytes = encryptionResult.encrypted.toByteArray(StandardCharsets.US_ASCII)
        val bytesSize = fileBytes.size.toLong()
        logger.info("Writing file 's3://$s3BucketName/$fullFilePath' of '$bytesSize' bytes.")

        val inputStream = ByteArrayInputStream(fileBytes)
        val bufferedInputStream = BufferedInputStream(inputStream)

        // eu-west-1 -> EU_WEST_2 (i.e tf style to enum name)


        // i.e. /mongo-export-2019-06-23/db.user.data-0001.bz2.enc
        // i.e. /mongo-export-2019-06-23/db.user.data-0001.metadata
        val objKeyName: String = fullFilePath.toString()

        try {
            //This code expects that you have AWS credentials set up per:
            // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html

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


    override fun outputPath(number: Int): Path {
        return Paths.get("""$s3PrefixFolder/$tableName-%06d.txt${if (compressOutput) ".bz2" else ""}${if (encryptOutput) ".enc" else ""}"""
                .format(number))
    }



    companion object {
        val logger: Logger = LoggerFactory.getLogger(S3DirectoryWriter::class.toString())
    }
}
