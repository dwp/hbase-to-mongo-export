package app.services.impl

import app.services.TableService
import app.utils.PropertyUtility
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class TableServiceImpl(private val dynamoDB: AmazonDynamoDB): TableService {

    override fun statuses(): List<String> {
        return statusTable().query(statusQuerySpec()).map(::collectionStatus)
    }

    private fun statusTable(): Table = DynamoDB(dynamoDB).getTable(statusTableName)
    private fun collectionStatus(item: Item): String = item[COLLECTION_STATUS_ATTRIBUTE_NAME] as String


    private fun statusQuerySpec(): QuerySpec =
        QuerySpec().apply {
            withKeyConditionExpression("#cId = :s")
            withNameMap(mapOf("#cId" to "CorrelationId"))
            withValueMap(mapOf(":s" to PropertyUtility.correlationId()))
        }

    @Value("\${dynamodb.status.table.name:UCExportToCrownStatus}")
    private lateinit var statusTableName: String

    companion object {
        private const val COLLECTION_STATUS_ATTRIBUTE_NAME = "CollectionStatus"
    }
}
