import app.configuration.LocalStackConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import io.kotlintest.shouldBe
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringRunner
import util.*

@RunWith(SpringRunner::class)
@ContextConfiguration(classes = [LocalStackConfiguration::class])
@ActiveProfiles("localstackConfiguration")
class BlockedTopicIntegrationTest {

    @Autowired
    private lateinit var amazonDynamoDb: AmazonDynamoDB

    @Test
    fun dynamoDBShouldHaveBlockedTopicRecord() {

        val tableName = "UCExportToCrownStatus"

        val correlationIdAttributeValue = correlationId()

        val collectionNameAttributeValue = blockedTopicAttributeValue()

        val primaryKey = primaryKeyMap(correlationIdAttributeValue, collectionNameAttributeValue)

        val getItemRequest = getItemRequest(tableName, primaryKey)

        val result = amazonDynamoDb.getItem(getItemRequest)
        val item = result.item
        val status = item["CollectionStatus"]?.s

        val expectedCollectionStatus = "Blocked_Topic"

        status shouldBe expectedCollectionStatus
    }

    private fun correlationId() = AttributeValue().apply {
        s = "blocked_topic"
    }
}
