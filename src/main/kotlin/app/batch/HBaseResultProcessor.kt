package app.batch

import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.MissingFieldException
import app.utils.TextUtils
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.springframework.batch.item.ItemProcessor
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.nio.charset.Charset

@Component
class HBaseResultProcessor(private val textUtils: TextUtils): ItemProcessor<Result, SourceRecord> {

    override fun process(result: Result): SourceRecord? {
        try {
            val idBytes = result.row
            val value = result.value()
            val json = value.toString(Charset.defaultCharset())
            val dataBlock = Gson().fromJson(json, JsonObject::class.java)
            val outerType = dataBlock.getAsJsonPrimitive("@type")?.asString ?: ""
            val messageInfo = dataBlock.getAsJsonObject("message")
            val innerType = messageInfo.getAsJsonPrimitive("@type")?.asString ?: ""
            val encryptedDbObject = messageInfo.getAsJsonPrimitive("dbObject")?.asString
            val encryptionInfo = messageInfo.getAsJsonObject("encryption")
            val _lastModifiedDateTime = messageInfo["_lastModifiedDateTime"]
            val lastModifiedDateTime =
                if (_lastModifiedDateTime != null && !_lastModifiedDateTime.isJsonNull && _lastModifiedDateTime is JsonPrimitive) {
                    messageInfo.getAsJsonPrimitive("_lastModifiedDateTime").asString
                } else {
                    ""
                }
            val encryptedEncryptionKey = encryptionInfo.getAsJsonPrimitive("encryptedEncryptionKey").asString
            val keyEncryptionKeyId = encryptionInfo.getAsJsonPrimitive("keyEncryptionKeyId").asString
            val initializationVector = encryptionInfo.getAsJsonPrimitive("initialisationVector").asString
            val (db, collection) = getDatabaseAndCollection(messageInfo)
            validateMandatoryField(encryptedDbObject, idBytes, "dbObject")
            validateMandatoryField(keyEncryptionKeyId, idBytes, "keyEncryptionKeyId")
            validateMandatoryField(initializationVector, idBytes, "initializationVector")
            validateMandatoryField(encryptedEncryptionKey, idBytes, "encryptedEncryptionKey")
            validateMandatoryField(db, idBytes, "db")
            validateMandatoryField(collection, idBytes, "collection")
            val encryptionBlock = EncryptionBlock(keyEncryptionKeyId, initializationVector, encryptedEncryptionKey)

            // Note that should this should never be set to 'full', or if it is it should be
            // strictly temporary and reverted as soon as possible. Logging the ids of all records
            // in the full export floods cloudwatch with billions of messages which significantly
            // slows down attempt to run queries.
            if (snapshotType == "incremental") {
                logger.info("Record read from hbase", "key" to printableKey(idBytes))
            }

            return SourceRecord(idBytes, encryptionBlock, encryptedDbObject!!, timestamp(result), db!!, collection!!,
                if (StringUtils.isNotBlank(outerType)) outerType else "TYPE_NOT_SET",
                if (StringUtils.isNotBlank(innerType)) innerType else "TYPE_NOT_SET", lastModifiedDateTime)
        } catch (e: Exception) {
            logger.error("Error in result processing", e)
            throw e
        }
    }

    fun printableKey(key: ByteArray): String {
        val hash = key.slice(IntRange(0, 3))
        val hex = hash.joinToString("") { String.format("\\x%02x", it) }
        val renderable = key.slice(IntRange(4, key.size - 1)).map{ it.toChar() }.joinToString("")
        return "${hex}${renderable}"
    }

    private fun timestamp(result: Result): Long =
        result.getColumnLatestCell(columnFamily, columnQualifier).timestamp

    private fun getDatabaseAndCollection(messageInfo: JsonObject): Pair<String?, String?> {
        var db = messageInfo.getAsJsonPrimitive("db")?.asString
        var collection = messageInfo.getAsJsonPrimitive("collection")?.asString

        if (db.isNullOrEmpty() || collection.isNullOrEmpty()) {
            val matcher = textUtils.topicNameTableMatcher(topicName)
            if (db.isNullOrEmpty()) {
                db = matcher?.groupValues?.get(1)
            }
            if (collection.isNullOrEmpty()) {
                collection = matcher?.groupValues?.get(2)
            }
        }

        return Pair(db, collection)
    }


    private fun validateMandatoryField(mandatoryFieldValue: String?, idBytes: ByteArray, fieldName: String) {
        if (mandatoryFieldValue.isNullOrEmpty()) {
            logger.error("Missing field, skipping this record", "id_bytes" to "$idBytes", "field_name" to fieldName)
            throw MissingFieldException(idBytes, fieldName)
        }
    }

    companion object {
        private val logger = DataworksLogger.getLogger(HBaseResultProcessor::class)
        private val columnFamily = Bytes.toBytes("cf")
        private val columnQualifier = Bytes.toBytes("record")
        fun printableKey(key: ByteArray): String {
            val hash = key.slice(IntRange(0, 3))
            val hex = hash.joinToString("") { String.format("\\x%02x", it) }
            val renderable = key.slice(IntRange(4, key.size - 1)).map { it.toChar() }.joinToString("")
            return "${hex}${renderable}"
        }
    }

    @Value("\${topic.name}")
    private var topicName: String = ""

    @Value("\${snapshot.type}")
    private lateinit var snapshotType: String
}
