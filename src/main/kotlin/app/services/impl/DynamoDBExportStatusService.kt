package app.services.impl

import app.services.ExportStatusService
import app.utils.PropertyUtility.correlationId
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest
import org.springframework.beans.factory.annotation.Value
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
class DynamoDBExportStatusService(private val dynamoDB: AmazonDynamoDB) : ExportStatusService {

    @Retryable(value = [Exception::class],
        maxAttemptsExpression = "\${dynamodb.retry.maxAttempts:5}",
        backoff = Backoff(delayExpression = "\${dynamodb.retry.delay:1000}",
            multiplierExpression = "\${dynamodb.retry.multiplier:2}"))
    override fun exportCompletedSuccessfully(): Boolean =
        exportStatusTable().query(statusQuerySpec()).map(::collectionStatus).all(::collectionCompletedSuccessfully)

    @Retryable(value = [Exception::class],
            maxAttemptsExpression = "\${dynamodb.retry.maxAttempts:5}",
            backoff = Backoff(delayExpression = "\${dynamodb.retry.delay:1000}",
                    multiplierExpression = "\${dynamodb.retry.multiplier:2}"))
    override fun incrementExportedCount(exportedFile: String) {
        val result = dynamoDB.updateItem(incrementFilesExportedRequest())
        logger.info("Incremented exported count",
                "file_exported" to exportedFile,
                "files_exported" to "${result.attributes[EXPORTED_FILE_COUNT_ATTRIBUTE_NAME]?.n}")
    }

    @Retryable(value = [Exception::class],
        maxAttemptsExpression = "\${dynamodb.retry.maxAttempts:5}",
        backoff = Backoff(delayExpression = "\${dynamodb.retry.delay:1000}",
            multiplierExpression = "\${dynamodb.retry.multiplier:2}"))
    override fun exportedFilesCount(): Int {
        return dynamoDB.getItem(getExportedCountRequest()).item[EXPORTED_FILE_COUNT_ATTRIBUTE_NAME]?.n?.toInt() ?: -1
    }

    private fun getExportedCountRequest(): GetItemRequest =
        GetItemRequest().apply {
            tableName = statusTableName
            key = primaryKey
            setAttributesToGet(listOf(EXPORTED_FILE_COUNT_ATTRIBUTE_NAME))
        }

    @Retryable(value = [Exception::class],
            maxAttemptsExpression = "\${dynamodb.retry.maxAttempts:5}",
            backoff = Backoff(delayExpression = "\${dynamodb.retry.delay:1000}",
                    multiplierExpression = "\${dynamodb.retry.multiplier:2}"))
    override fun setExportedStatus() = setStatus("Exported")

    @Retryable(value = [Exception::class],
            maxAttemptsExpression = "\${dynamodb.retry.maxAttempts:5}",
            backoff = Backoff(delayExpression = "\${dynamodb.retry.delay:1000}",
                    multiplierExpression = "\${dynamodb.retry.multiplier:2}"))
    override fun setFailedStatus() = setStatus("Export_Failed")

    @Retryable(value = [Exception::class],
            maxAttemptsExpression = "\${dynamodb.retry.maxAttempts:5}",
            backoff = Backoff(delayExpression = "\${dynamodb.retry.delay:1000}",
                    multiplierExpression = "\${dynamodb.retry.multiplier:2}"))
    override fun setTableUnavailableStatus() = setStatus("Table_Unavailable")

    @Retryable(value = [Exception::class],
            maxAttemptsExpression = "\${dynamodb.retry.maxAttempts:5}",
            backoff = Backoff(delayExpression = "\${dynamodb.retry.delay:1000}",
                    multiplierExpression = "\${dynamodb.retry.multiplier:2}"))
    override fun setBlockedTopicStatus() = setStatus("Blocked_Topic")

    private fun setStatus(status: String) {
        val result = dynamoDB.updateItem(setCollectionStatusRequest(status))
        logger.info("Collection status set",
                "collection_status" to "${result.attributes["CollectionStatus"]}",
                "files_exported" to "${result.attributes[EXPORTED_FILE_COUNT_ATTRIBUTE_NAME]?.n}",
                "files_sent" to "${result.attributes["FilesSent"]?.n}")
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

    private fun collectionStatus(item: Item): String = item[COLLECTION_STATUS_ATTRIBUTE_NAME] as String

    private fun collectionCompletedSuccessfully(status: Any): Boolean =
        status == "Exported" || status == "Sent" || status == "Received" || status == "Success"

    private fun exportStatusTable(): Table = DynamoDB(dynamoDB).getTable(statusTableName)

    private fun statusQuerySpec(): QuerySpec =
        QuerySpec().apply {
            withKeyConditionExpression("#cId = :s")
            withNameMap(mapOf("#cId" to "CorrelationId"))
            withValueMap(mapOf(":s" to correlationId()))
        }

    private val primaryKey by lazy {
        val correlationIdAttributeValue = AttributeValue().apply { s = correlationId() }
        val collectionNameAttributeValue = AttributeValue().apply { s = topicName }
        mapOf("CorrelationId" to correlationIdAttributeValue, "CollectionName" to collectionNameAttributeValue)
    }

    @Value("\${dynamodb.status.table.name:UCExportToCrownStatus}")
    private lateinit var statusTableName: String

    @Value("\${topic.name}")
    private lateinit var topicName: String

    companion object {
        val logger = DataworksLogger.getLogger(DynamoDBExportStatusService::class)
        private const val EXPORTED_FILE_COUNT_ATTRIBUTE_NAME = "FilesExported"
        private const val COLLECTION_STATUS_ATTRIBUTE_NAME = "CollectionStatus"
    }
}
