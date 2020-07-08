package app.services.impl

import app.services.ExportStatusService
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service

@Service
class DynamoDBExportStatusService(private val dynamoDB: AmazonDynamoDB) : ExportStatusService {

    @Retryable(value = [Exception::class],
            maxAttempts = maxAttempts,
            backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    override fun incrementExportedCount() {
        logger.info("Incrementing exported count")
        val result = dynamoDB.updateItem(incrementFilesExportedRequest())
        logger.info("Incremented exported count",  "files_exported", result.attributes["FilesExported"])
    }

    @Retryable(value = [Exception::class],
            maxAttempts = maxAttempts,
            backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    @Synchronized
    override fun setExportedStatus() {
        val req = setStatusExportedRequest()
        logger.info("Updating exported status", "request", "$req")
        val result = dynamoDB.updateItem(req)
        logger.info("Update CollectionStatus: ${result.attributes["CollectionStatus"]}")
    }

    private fun incrementFilesExportedRequest() =
            UpdateItemRequest().apply {
                tableName = statusTableName
                key = primaryKey
                updateExpression = "SET FilesExported = FilesExported + :x"
                expressionAttributeValues = mapOf(":x" to AttributeValue().apply { n = "1" })
                returnValues = "ALL_NEW"
            }

    private fun setStatusExportedRequest() =
            UpdateItemRequest().apply {
                tableName = statusTableName
                key = primaryKey
                updateExpression = "SET CollectionStatus = :x"
                expressionAttributeValues = mapOf(":x" to AttributeValue().apply { s = "Exported" })
                returnValues = "ALL_NEW"
            }

    private val primaryKey by lazy {
        val correlationIdAttributeValue = AttributeValue().apply { s = correlationId }
        val collectionNameAttributeValue = AttributeValue().apply { s = topicName }
        mapOf("CorrelationId" to correlationIdAttributeValue, "CollectionName" to collectionNameAttributeValue)
    }

    @Value("\${dynamodb.status.table.name:UCExportToCrownStatus}")
    private lateinit var statusTableName: String

    @Value("\${topic.name}")
    private lateinit var topicName: String

    private val correlationId by lazy { System.getProperty("correlation_id") }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DynamoDBExportStatusService::class.toString())
        const val maxAttempts = 5
        const val initialBackoffMillis = 1000L
        const val backoffMultiplier = 2.0
    }
}
