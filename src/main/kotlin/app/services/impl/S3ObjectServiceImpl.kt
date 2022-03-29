package app.services.impl

import app.domain.EncryptingOutputStream
import app.services.S3ObjectService
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import io.prometheus.client.Counter
import io.prometheus.client.spring.web.PrometheusTimeMethod
import org.springframework.beans.factory.annotation.Value
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import java.io.ByteArrayInputStream

@Service
class S3ObjectServiceImpl(private val amazonS3: AmazonS3, private val retriedBatchPutCounter: Counter): S3ObjectService {

    @Retryable(value = [Exception::class],
        maxAttemptsExpression = "\${s3.retry.maxAttempts:5}",
        backoff = Backoff(delayExpression = "\${s3.retry.delay:1000}",
            multiplierExpression = "\${s3.retry.multiplier:2}"))
    @PrometheusTimeMethod(name = "htme_s3_batch_put_operation_duration", help = "Duration of batch s3 puts")
    override fun putObject(objectKey: String, encryptingOutputStream: EncryptingOutputStream) {
        try {
            ByteArrayInputStream(encryptingOutputStream.data()).use {
                amazonS3.putObject(PutObjectRequest(exportBucket, objectKey, it,
                    objectMetadata(objectKey, encryptingOutputStream)))
            }
        } catch (e: Exception) {
            retriedBatchPutCounter.inc()
            throw e
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
               addUserMetadata("data_product", topicName.replace("db.", ""))
               addUserMetadata("data_product_type", snapshotType)
               contentLength = encryptingOutputStream.data().size.toLong()
           }

    @Value("\${s3.bucket}")
    private lateinit var exportBucket: String

    @Value("\${snapshot.type}")
    private lateinit var snapshotType: String

    @Value("\${topic.name}")
    private lateinit var topicName: String
}
