package app.services.impl

import app.domain.EncryptingOutputStream
import app.services.S3ObjectService
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.springframework.beans.factory.annotation.Value
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import java.io.ByteArrayInputStream

@Service
class S3ObjectServiceImpl(private val amazonS3: AmazonS3): S3ObjectService {

    @Retryable(value = [Exception::class],
        maxAttemptsExpression = "\${s3.retry.maxAttempts:5}",
        backoff = Backoff(delayExpression = "\${s3.retry.delay:1000}",
            multiplierExpression = "\${s3.retry.multiplier:2}"))
    override fun putObject(objectKey: String, encryptingOutputStream: EncryptingOutputStream) {
        ByteArrayInputStream(encryptingOutputStream.data()).use {
            amazonS3.putObject(PutObjectRequest(exportBucket, objectKey, it,
                objectMetadata(objectKey, encryptingOutputStream)))
        }
    }

    private fun objectMetadata(objectKey: String,
                               encryptingOutputStream: EncryptingOutputStream): ObjectMetadata =
            ObjectMetadata().apply {
               contentType = "binary/octetstream"
               addUserMetadata("x-amz-meta-title", objectKey)
               addUserMetadata("iv", encryptingOutputStream.initialisationVector)
               addUserMetadata("cipherText", encryptingOutputStream.dataKeyResult.ciphertextDataKey)
               addUserMetadata("dataKeyEncryptionKeyId", encryptingOutputStream.dataKeyResult.dataKeyEncryptionKeyId)
               contentLength = encryptingOutputStream.data().size.toLong()
           }

    @Value("\${s3.bucket}")
    private lateinit var exportBucket: String
}
