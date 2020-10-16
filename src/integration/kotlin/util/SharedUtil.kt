package util

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemRequest

fun integrationTestCorrelationId() = AttributeValue().apply {
    s = "integration_test_correlation_id"
}

fun blockedTopicAttributeValue() = AttributeValue().apply {
    s = "db.blocked.topic"
}

fun doesNotExistAttributeValue() = AttributeValue().apply {
    s = "does.not.exist"
}

fun primaryKeyMap(correlationIdAttributeValue: AttributeValue, collectionNameAttributeValue: AttributeValue) = mapOf("CorrelationId" to correlationIdAttributeValue,
        "CollectionName" to collectionNameAttributeValue)

fun getItemRequest(primaryKey: Map<String, AttributeValue>) = GetItemRequest().apply {
    tableName = "UCExportToCrownStatus"
    key = primaryKey
}
