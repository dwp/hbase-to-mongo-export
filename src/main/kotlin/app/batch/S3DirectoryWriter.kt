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
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

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
            if (batchSize + item.length > maxBatchOutputSize) {
                writeOutput()
            }
            currentBatch.append(item)
            batchSize += item.length
        }
    }

    fun writeOutput() {
        if (batchSize > 0) {

            val dataFile = outputName(++currentOutputFileNumber)

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

                val metadataFile = metadataPath()
                val metadataByteArrayOutputStream = ByteArrayOutputStream()
                val metadataStream: OutputStream = BufferedOutputStream(metadataByteArrayOutputStream)
                metadataStream.use {
                    val iv = encryptionResult.initialisationVector
                    val plaintext = dataKeyResult.plaintextDataKey //TODO ask Dan C about this
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
            this.batchSize = 0
        }
    }

    private fun writeToS3(fullLocalFilePath: String, fileBytes: ByteArray) {
        // See also https://github.com/aws/aws-sdk-java

        val inputStream = ByteArrayInputStream(fileBytes)
        val bufferedInputStream = BufferedInputStream(inputStream)

        val clientRegion = Regions.valueOf(region)

        // i.e. /mongo-export-2019-06-23/db.user.data-0001.bz2.enc
        // i.e. /mongo-export-2019-06-23/db.user.data-0001.metadata
        val objKeyName: String = fullLocalFilePath.toString()

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
            metadata.contentLength = fileBytes.size.toLong()
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

    private fun bufferedOutputStream(outputStream: OutputStream): OutputStream =
            if (compressOutput) {
                CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2,
                        BufferedOutputStream(outputStream))
            } else {
                BufferedOutputStream(outputStream)
            }

    private fun metadataPath() =
              """$s3FolderPrefix/$tableName-%04d.metadata""".format(currentOutputFileNumber)


    private var currentBatch = StringBuilder()
    private var batchSize = 0

    private fun outputName(number: Int) =
            """$s3FolderPrefix/$tableName-%04d.txt${if (compressOutput) ".bz2" else ""}${if (encryptOutput) ".enc" else ""}"""
                    .format(number)


    private var currentOutputFileNumber = 0

    @Value("\${output.batch.size.max}")
    private var maxBatchOutputSize: Int = 0

    @Value("\${aws.region}")
    private var region: String = "eu-west-1"

    @Value("\${s3.bucket}")
    private lateinit var s3BucketName: String // i.e. "1234567890"

    @Value("\${s3.folder}")
    private lateinit var s3FolderPrefix: String //i.e. "mongo-export-2019-06-23"

    @Value("\${source.table.name}")
    private lateinit var tableName: String // i.e. "db.user.data"

    @Value("\${compress.output:false}")
    private var compressOutput: Boolean = true

    @Value("\${encrypt.output:true}")
    private var encryptOutput: Boolean = true

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DirectoryWriter::class.toString())
    }
}
