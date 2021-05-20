package app.services.impl

import app.services.ExportCompletionStatus
import app.services.ProductStatusService
import app.utils.PropertyUtility.correlationId
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest
import org.springframework.beans.factory.annotation.Value
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
class DynamoDBProductStatusService(private val dynamoDB: AmazonDynamoDB): ProductStatusService {

    @Retryable(value = [Exception::class],
        maxAttemptsExpression = "\${dynamodb.retry.maxAttempts:5}",
        backoff = Backoff(delayExpression = "\${dynamodb.retry.delay:1000}",
            multiplierExpression = "\${dynamodb.retry.multiplier:2}"))
    override fun setFailedStatus() {
        setStatus("FAILED")
    }

    @Retryable(value = [Exception::class],
        maxAttemptsExpression = "\${dynamodb.retry.maxAttempts:5}",
        backoff = Backoff(delayExpression = "\${dynamodb.retry.delay:1000}",
            multiplierExpression = "\${dynamodb.retry.multiplier:2}"))
    override fun setCompletedStatus() = setStatus("COMPLETED")

    private fun setStatus(status: String) {
        val result = dynamoDB.updateItem(setProductStatusRequest(status))
        logger.info("Product status set",
            "product_status" to "${result.attributes["Status"]}")
    }

    private fun setProductStatusRequest(status: String) =
        UpdateItemRequest().apply {
            tableName = productTableName
            key = primaryKey
            updateExpression = "SET Status = :x"
            expressionAttributeValues = mapOf(":x" to AttributeValue().apply { s = status })
            returnValues = "ALL_NEW"
        }

    private val primaryKey by lazy {
        val correlationIdAttributeValue = AttributeValue().apply { s = correlationId() }
        val dataProductAttributeValue = AttributeValue().apply { s = DATA_PRODUCT_VALUE }
        mapOf("Correlation_Id" to correlationIdAttributeValue, "DataProduct" to dataProductAttributeValue)
    }

    @Value("\${dynamodb.product.status.table.name:data_pipeline_metadata}")
    private lateinit var productTableName: String

    companion object {
        val logger = DataworksLogger.getLogger(DynamoDBProductStatusService::class)
        private const val DATA_PRODUCT_VALUE = "HTME"
    }
}
