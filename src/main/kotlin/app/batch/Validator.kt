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
                val originalId = if (idElement is JsonObject) {
                    idElement.toString()
                } else {
                    idElement.asString
                }

                val dbObjectWithId = if (idElement is JsonObject) {
                    dbObject
                } else {
                    replaceElementValueWithKeyValuePair(dbObject, "_id", "\$oid", originalId)
                }

                val (dbObjectWithWrappedDates, lastModifiedDate) = wrapDates(dbObjectWithId)

                val newIdElement = dbObjectWithWrappedDates["_id"]
                val newIdAsString = if (newIdElement is JsonObject) {
                    newIdElement.toString()
                } else {
                    newIdElement.asString
                }

                val timeAsLong = timestampAsLong(lastModifiedDate)
                val manifestRecord = ManifestRecord(newIdAsString, 
                    timeAsLong, db, collection, "EXPORT", item.type, originalId)
                
                    dbObjectWithWrappedDates.addProperty("timestamp", item.hbaseTimestamp)
                return DecryptedRecord(dbObjectWithWrappedDates, manifestRecord)
            }
        } catch (e: Exception) {
            e.printStackTrace(System.err)
            logError(logger, "Error decrypting record", e, "exceptionMessage", e.message ?: "No message", "is_blank", "${StringUtils.isBlank(decrypted)}", "hbase_row_id", printableKey(item.hbaseRowId), "db_name", db, "collection_name", collection)
            throw BadDecryptedDataException(hbaseRowId, db, collection, e.message ?: "No exception message")
        }
        return null
    }

    fun wrapDates(objectWithDatesIn: JsonObject): Pair<JsonObject, String> {
        val lastModifiedDateTimeAsString = retrieveLastModifiedDateTime(objectWithDatesIn)
        val dbObjectWithLastModifiedDate = replaceElementValueWithKeyValuePair(
            objectWithDatesIn, 
            "_lastModifiedDateTime", 
            "\$date", 
            lastModifiedDateTimeAsString)

        val createdDateTimeAsString = retrieveDateTimeElement("createdDateTime", objectWithDatesIn)
        val dbObjectWithLastModifiedAndCreatedDate = if (!StringUtils.isEmpty(createdDateTimeAsString)) { 
            replaceElementValueWithKeyValuePair(
                dbObjectWithLastModifiedDate, 
                "createdDateTime", 
                "\$date", 
                createdDateTimeAsString) 
            } else { dbObjectWithLastModifiedDate }

        val removedDateTimeAsString = retrieveDateTimeElement("_removedDateTime", objectWithDatesIn)
        val dbObjectWithAllDates = if (!StringUtils.isEmpty(removedDateTimeAsString)) { 
            replaceElementValueWithKeyValuePair(
                dbObjectWithLastModifiedAndCreatedDate, 
                "_removedDateTime", 
                "\$date", 
                removedDateTimeAsString) 
            } else { dbObjectWithLastModifiedAndCreatedDate }

        return Pair(dbObjectWithAllDates, lastModifiedDateTimeAsString)
    }

    fun replaceElementValueWithKeyValuePair(objectWithFieldIn: JsonObject, keyToReplace: String, newKey: String, value: String): JsonObject {
        var objectWithChangedField = objectWithFieldIn
        val newElement = JsonObject()
        newElement.addProperty(newKey, value)
        objectWithChangedField.remove(keyToReplace)
        objectWithChangedField.add(keyToReplace, newElement)
        return objectWithChangedField
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

    fun retrieveLastModifiedDateTime(jsonObject: JsonObject): String {
        val epoch = "1980-01-01T00:00:00.000Z"
        val lastModifiedDateTime = retrieveDateTimeElement("_lastModifiedDateTime", jsonObject)
        val createdDateTime = retrieveDateTimeElement("createdDateTime", objectWithDatesIn)
        
        if (!StringUtils.isBlank(lastModifiedDateTime)) {
            return lastModifiedDateTime
        }
        
        if (!StringUtils.isBlank(createdDateTime)) {
            return createdDateTime
        }

        return epoch
    }

    fun retrieveDateTimeElement(key: String, jsonObject: JsonObject): String {
        val dateElement = jsonObject[key]
        if (dateElement != null) {
            if (dateElement is JsonObject) {
                if (dateElement["\$date"] != null) {
                    return dateElement["\$date"].asString
                }
            } else {
                return dateElement.asString
            }
        }

        return ""
    }

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
