package app.batch

import app.domain.Record
import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import org.springframework.batch.item.ItemProcessor
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * Perform any collection specific transformations prior to output.
 *
 * Currently, only equality data needs this
 */
@Component
class TransformationProcessor: ItemProcessor<Record, Record> {

    /**
     * Perform any collection specific transformations prior to output.
     */
    override fun process(item: Record): Record? =
        when (topicName) {
            EQUALITY_TOPIC -> {
                transformedEqualityRecord(item)
            }
            else -> {
                item
            }
        }

    private fun transformedEqualityRecord(item: Record): Record =
        Record(gson.toJson(wrapped(item)), item.manifestRecord)

    private fun wrapped(item: Record): JsonObject =
        JsonObject().apply { add("message", wrappedObject(item)) }

    private fun wrappedObject(item: Record): JsonObject =
        gson.fromJson(item.dbObjectAsString, JsonObject::class.java).apply {
            addProperty("@type", item.manifestRecord.externalInnerSource)
        }

    @Value("\${topic.name}")
    private lateinit var topicName: String

    companion object {
        private const val EQUALITY_TOPIC = "data.equality"
        private val gson = GsonBuilder().serializeNulls().create()
    }
}
