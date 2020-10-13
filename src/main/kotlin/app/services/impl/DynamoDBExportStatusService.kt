package app.services.impl

import app.services.ExportStatusService
import app.utils.logging.logInfo
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
    override fun incrementExportedCount(exportedFile: String) {
        val result = dynamoDB.updateItem(incrementFilesExportedRequest())
        logInfo(logger, "Incremented exported count",
                "file_exported", exportedFile,
                "files_exported", "${result.attributes["FilesExported"]?.n}")
    }

    @Retryable(value = [Exception::class],
            maxAttempts = maxAttempts,
            backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    override fun setExportedStatus() = setStatus("Exported")

    @Retryable(value = [Exception::class],
            maxAttempts = maxAttempts,
            backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    override fun setFailedStatus() = setStatus("Export_Failed")

    @Retryable(value = [Exception::class],
            maxAttempts = maxAttempts,
            backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    override fun setTableUnavailableStatus() = setStatus("Table_Unavailable")

    private fun setStatus(status: String) {
        val result = dynamoDB.updateItem(setCollectionStatusRequest(status))
        logInfo(logger, "Collection status set",
                "collection_status", "${result.attributes["CollectionStatus"]}")
    }

    private fun incrementFilesExportedRequest() =
            UpdateItemRequest().apply {
                tableName = statusTableName
                key = primaryKey
                updateExpression = "SET FilesExported = FilesExported + :x"
                expressionAttributeValues = mapOf(":x" to AttributeValue().apply { n = "1" })
                returnValues = "ALL_NEW"
            }

    private fun setCollectionStatusRequest(status: String) =
            UpdateItemRequest().apply {
                tableName = statusTableName
                key = primaryKey
                updateExpression = "SET CollectionStatus = :x"
                expressionAttributeValues = mapOf(":x" to AttributeValue().apply { s = status })
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

    private val correlationId by lazy { System.getProperty("correlation_id", "NOT_SET") }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DynamoDBExportStatusService::class.toString())
        const val maxAttempts = 5
        const val initialBackoffMillis = 1000L
        const val backoffMultiplier = 2.0
    }
}
