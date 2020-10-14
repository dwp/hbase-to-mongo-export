import app.configuration.LocalStackConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.s3.AmazonS3
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@ContextConfiguration(classes = [LocalStackConfiguration::class])
@ActiveProfiles("localstackConfiguration")
class TableUnavailableIntegrationTest {

    @Autowired
    private lateinit var amazonDynamoDb: AmazonDynamoDB

    @Test
    fun dynamoDBShouldHaveTableUnavailableRecord() {

        val correlationIdAttributeValue = AttributeValue().apply {
            s = "integration_test_correlation_id"
        }
        val collectionNameAttributeValue = AttributeValue().apply {
            s = "db.penalties-and-deductions.sanction"
        }
        val primaryKey = mapOf("CorrelationId" to correlationIdAttributeValue,
                "CollectionName" to collectionNameAttributeValue)

        val getItemRequest = GetItemRequest().apply {
            tableName = "UCExportToCrownStatus"
            key = primaryKey
        }
        val result = amazonDynamoDb.getItem(getItemRequest)
        val item = result.item
        val status = item["CollectionStatus"]

        val expectedCollectionStatus = "Table_Unavailable"

        assertThat(status).isEqualTo(expectedCollectionStatus)
    }

    companion object {
        private const val tableName = "UCExportToCrownStatus"
        private const val partitionKey = "table-does-not-exist"
    }
}
