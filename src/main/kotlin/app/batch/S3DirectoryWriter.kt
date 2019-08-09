package app.batch

import app.services.CipherService
import app.services.KeyService
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.*
import java.nio.charset.StandardCharsets

import com.amazonaws.AmazonServiceException
import com.amazonaws.SdkClientException
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest

// See also https://github.com/aws/aws-sdk-java

@Component
@Profile("outputToS3")
class S3DirectoryWriter(private val keyService: KeyService,
                        private val cipherService: CipherService) : ItemWriter<String> {

    override fun write(items: MutableList<out String>) {
        items.map { "$it\n" }.forEach { item ->
            if (batchSizeBytes + item.length > maxBatchOutputSizeBytes) {
                writeOutput()
            }
            currentBatch.append(item)
            batchSizeBytes += item.length
        }
    }

    fun writeOutput() {
        if (batchSizeBytes > 0) {

            val dataFile = outputName(++currentOutputFileNumber)
            logger.info("Processing file number '%06d' with batchSizeBytes='$batchSizeBytes'.".format(currentOutputFileNumber))

            if (encryptOutput) {
                val dataKeyResult = keyService.batchDataKey()
                logger.info("dataKeyResult: '$dataKeyResult'.")
                val byteArrayOutputStream = ByteArrayOutputStream()

                bufferedOutputStream(byteArrayOutputStream).use {
                    it.write(this.currentBatch.toString().toByteArray(StandardCharsets.UTF_8))
                }

                val encryptionResult =
                        this.cipherService.encrypt(dataKeyResult.plaintextDataKey,
                                byteArrayOutputStream.toByteArray())

                val dataBytes = encryptionResult.encrypted.toByteArray(StandardCharsets.US_ASCII)
                writeToS3(dataFile, dataBytes)

                val metadataFile = metadataPath(currentOutputFileNumber)
                val metadataByteArrayOutputStream = ByteArrayOutputStream()
                val metadataStream: OutputStream = BufferedOutputStream(metadataByteArrayOutputStream)
                metadataStream.use {
                    val iv = encryptionResult.initialisationVector
                    //val plaintext = dataKeyResult.plaintextDataKey //TODO ask Dan C about this
                    it.write("iv=$iv\n".toByteArray(StandardCharsets.UTF_8))
                    it.write("ciphertext=${dataKeyResult.ciphertextDataKey}\n".toByteArray(StandardCharsets.UTF_8))
                    it.write("dataKeyEncryptionKeyId=${dataKeyResult.dataKeyEncryptionKeyId}\n".toByteArray(StandardCharsets.UTF_8))
                }
                val metadataBytes = metadataByteArrayOutputStream.toByteArray()
                writeToS3(metadataFile, metadataBytes)

            } else {
                //no encryption
                val byteArrayOutputStream = ByteArrayOutputStream()

                bufferedOutputStream(byteArrayOutputStream).use {
                    it.write(this.currentBatch.toString().toByteArray(StandardCharsets.UTF_8))
                }
                writeToS3(dataFile, byteArrayOutputStream.toByteArray())
            }

            this.currentBatch = StringBuilder()
            this.batchSizeBytes = 0
        }
    }

    private fun writeToS3(fullFilePath: String, fileBytes: ByteArray) {
        // See also https://github.com/aws/aws-sdk-java
        val bytesSize = fileBytes.size.toLong()
        val file = "s3://$s3BucketName/$fullFilePath"
        logger.info("Writing file '$file' of '$bytesSize' bytes.")

        val inputStream = ByteArrayInputStream(fileBytes)
        val bufferedInputStream = BufferedInputStream(inputStream)

        // eu-west-1 -> EU_WEST_2 (i.e tf style to enum name)
        val updated_region = region.toUpperCase().replace("-", "_")
        val clientRegion = Regions.valueOf(updated_region)

        // i.e. /mongo-export-2019-06-23/db.user.data-0001.bz2.enc
        // i.e. /mongo-export-2019-06-23/db.user.data-0001.metadata
        val objKeyName: String = fullFilePath.toString()

        try {
            //This code expects that you have AWS credentials set up per:
            // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
            val s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .build()

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
            logger.error("Error Writing file '$file'", e)
        } catch (e: SdkClientException) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace()
            logger.error("Error Writing file '$file'", e)
        }
    }

    private fun bufferedOutputStream(outputStream: OutputStream): OutputStream =
            if (compressOutput) {
                CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2,
                        BufferedOutputStream(outputStream))
            } else {
                BufferedOutputStream(outputStream)
            }

    private fun metadataPath(number: Int) =
              """$s3PrefixFolder/$topicName-%06d.metadata""".format(number)


    private var currentBatch = StringBuilder()
    private var batchSizeBytes = 0

    private fun outputName(number: Int) =
            """$s3PrefixFolder/$topicName-%06d.txt${if (compressOutput) ".bz2" else ""}${if (encryptOutput) ".enc" else ""}"""
                    .format(number)

    private var currentOutputFileNumber = 0

    @Value("\${output.batch.size.max.bytes}")
    private var maxBatchOutputSizeBytes: Int = 0

    @Value("\${aws.region}")
    private var region: String = "eu-west-1"

    @Value("\${s3.bucket}")
    private lateinit var s3BucketName: String // i.e. "1234567890"

    @Value("\${s3.prefix.folder}")
    private lateinit var s3PrefixFolder: String //i.e. "mongo-export-2019-06-23"

    @Value("\${topic.name}")
    private lateinit var topicName: String // i.e. "db.user.data"

    @Value("\${compress.output:true}")
    private var compressOutput: Boolean = true

    @Value("\${encrypt.output:true}")
    private var encryptOutput: Boolean = true

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DirectoryWriter::class.toString())
    }
}
