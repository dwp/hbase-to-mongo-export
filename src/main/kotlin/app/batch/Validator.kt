package app.batch

import app.domain.DecryptedRecord
import app.domain.ManifestRecord
import app.domain.SourceRecord
import app.exceptions.BadDecryptedDataException
import app.utils.logging.logDebug
import app.utils.logging.logError
import com.google.gson.Gson
import com.google.gson.JsonObject
import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.text.SimpleDateFormat
import java.util.*

@Component
class Validator {
    val validTimestamps = listOf("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val idNotFound = "_id field not found in the decrypted db object"

    fun skipBadDecryptedRecords(item: SourceRecord, decrypted: String): DecryptedRecord? {
        val hbaseRowKey = Arrays.copyOfRange(item.hbaseRowId, 4, item.hbaseRowId.size)
        val hbaseRowId = String(hbaseRowKey)
        val db = item.db
        val collection = item.collection
        try {
            val dbObject = parseDecrypted(decrypted)
            if (null != dbObject) {
                val idElement = retrieveId(dbObject)

                val (id, originalId) = if (idElement is JsonObject) {
                    Pair(idElement.toString(), idElement.toString())
                }
                else {
                    val reconstructedMongoId = JsonObject()
                    reconstructedMongoId.addProperty("\$oid", idElement.asString)
                    dbObject.remove("_id")
                    dbObject.add("_id", reconstructedMongoId)
                    Pair(reconstructedMongoId.toString(), idElement.asString)
                }

                val timeAsLong = timestampAsLong(item.lastModified)
                dbObject.addProperty("timestamp", item.hbaseTimestamp)
                val manifestRecord = ManifestRecord(id, timeAsLong, db, collection, "EXPORT", item.type, originalId)
                return DecryptedRecord(dbObject, manifestRecord)
            }
        } catch (e: Exception) {
            e.printStackTrace(System.err)
            logError(logger, "Error decrypting record", e, "exceptionMessage", e.message ?: "No message", "is_blank", "${StringUtils.isBlank(decrypted)}", "hbase_row_id", printableKey(item.hbaseRowId), "db_name", db, "collection_name", collection)
            throw BadDecryptedDataException(hbaseRowId, db, collection, e.message ?: "No exception message")
        }
        return null
    }

    fun parseDecrypted(decrypted: String): JsonObject? {
        return Gson().fromJson(decrypted, JsonObject::class.java)
    }

    fun printableKey(key: ByteArray): String {
        val hash = key.slice(IntRange(0, 3))
        val hex = hash.map { String.format("\\x%02x", it) }.joinToString("")
        val renderable = key.slice(IntRange(4, key.size - 1)).map{ it.toChar() }.joinToString("")
        return "${hex}${renderable}"
    }

    fun retrieveId(jsonObject: JsonObject) = jsonObject["_id"] ?: throw Exception(idNotFound)


    fun timestampAsLong(lastUpdatedTimestamp: String): Long {
        validTimestamps.forEach {
            try {
                val df = SimpleDateFormat(it)
                return df.parse(lastUpdatedTimestamp).time
            } catch (e: Exception) {
                logDebug(logger, "lastUpdatedTimestamp did not match valid formats", "last_updated_timestamp", lastUpdatedTimestamp, "failed_format", it)
            }
        }
        throw Exception("Unparseable date found: \"$lastUpdatedTimestamp\", did not match any of $validTimestamps")
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(Validator::class.toString())
    }
}
