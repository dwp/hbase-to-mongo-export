package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import app.services.CipherService
import app.services.KeyService
import com.amazonaws.AmazonServiceException
import com.amazonaws.SdkClientException
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths

// See also https://github.com/aws/aws-sdk-java

@Component
@Profile("outputToS3")
class S3DirectoryWriter(private val keyService: KeyService,
                        private val cipherService: CipherService) : Writer<String>(keyService, cipherService) {

    override fun writeData(encryptionResult: EncryptionResult, dataKeyResult: DataKeyResult) {
        // See also https://github.com/aws/aws-sdk-java
        val fullFilePath = outputPath(++currentOutputFileNumber)
        val fileBytes = encryptionResult.encrypted.toByteArray(StandardCharsets.US_ASCII)
        val dataBytesSize = fileBytes.size.toLong()
        val file = "s3://$s3BucketName/$fullFilePath"
        logger.info("Writing file '$file' of '$dataBytesSize' bytes.")

        val inputStream = ByteArrayInputStream(fileBytes)
        val dataInputStream = BufferedInputStream(inputStream)

        // eu-west-1 -> EU_WEST_2 (i.e tf style to enum name)
        val updatedRegion = region.toUpperCase().replace("-", "_")
        val clientRegion = Regions.valueOf(updatedRegion)

        // i.e. /mongo-export-2019-06-23/db.user.data-0001.bz2.enc
        // i.e. /mongo-export-2019-06-23/db.user.data-0001.metadata
        val dataKeyName: String = fullFilePath.toString()

        //This code expects that you have AWS credentials set up per:
        // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
        val s3Client = AmazonS3ClientBuilder.standard()
            .withRegion(clientRegion)
            .build()!!

        writeS3File(dataKeyName, dataInputStream, dataBytesSize, s3Client)

        val metadataPath = metadataPath(currentOutputFileNumber)
        val iv = encryptionResult.initialisationVector
        val metadataString = "iv=$iv\n" +
            "ciphertext=${dataKeyResult.ciphertextDataKey}\n" +
            "dataKeyEncryptionKeyId=${dataKeyResult.dataKeyEncryptionKeyId}\n"
        val metadataBytes = metadataString.toByteArray(StandardCharsets.US_ASCII)

        val metaDataInputStream = ByteArrayInputStream(metadataBytes)
        val metaDataBufferedInputStream = BufferedInputStream(metaDataInputStream)
        writeS3File(metadataPath, metaDataBufferedInputStream, metadataBytes.size.toLong(), s3Client)

    }

    private fun writeS3File(s3Key: String, inputStream: InputStream, bytesSize: Long, s3Client: AmazonS3){
        try {

            // Upload a file as a new object with ContentType and title specified.
            val s3MetaData = ObjectMetadata()
            s3MetaData.contentType = "binary/octetstream"
            s3MetaData.addUserMetadata("x-amz-meta-title", s3Key)
            s3MetaData.contentLength = bytesSize
            val dataRequest = PutObjectRequest(s3BucketName, s3Key, inputStream, s3MetaData)
            s3Client.putObject(dataRequest)
            logger.info("Wrote file '$s3Key'.")

        } catch (e: AmazonServiceException) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace()
            logger.error(
                "AWS Service Exception: Error Writing file '$s3Key': " +
                    "Call transmitted but failed to process:", e)
        } catch (e: SdkClientException) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace()
            logger.error("AWS SDK Client error: Error Writing file '$s3Key':", e)
        }
    }

    private fun metadataPath(number: Int) =
        """$s3PrefixFolder/$topicName-%06d.metadata""".format(number)

    private fun outputName(number: Int) =
        """$s3PrefixFolder/$topicName-%06d.txt${if (compressOutput) ".bz2" else ""}${if (encryptOutput) ".enc" else ""}"""
            .format(number)

    override fun outputPath(number: Int): Path {
        return Paths.get("""$s3PrefixFolder/$topicName-%06d.txt${if (compressOutput) ".bz2" else ""}${if (encryptOutput) ".enc" else ""}"""
            .format(number))
    }

    @Value("\${aws.region}")
    private lateinit var region: String

    @Value("\${s3.bucket}")
    private lateinit var s3BucketName: String // i.e. "1234567890"

    @Value("\${s3.prefix.folder}")
    private lateinit var s3PrefixFolder: String //i.e. "mongo-export-2019-06-23"

    companion object {
        val logger: Logger = LoggerFactory.getLogger(S3DirectoryWriter::class.toString())
    }
}
