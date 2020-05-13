package app.batch

import app.domain.DecryptedRecord
import app.domain.ManifestRecord
import app.domain.SourceRecord
import app.exceptions.BadDecryptedDataException
import app.utils.logging.logDebug
import app.utils.logging.logError
import com.google.gson.Gson
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.text.SimpleDateFormat
import java.util.*
import app.utils.JsonUtils

@Component
class Validator {
    private final val validIncomingFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"
    private final val validOutgoingFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    val validTimestamps = listOf(validIncomingFormat, validOutgoingFormat)
    val idNotFound = "_id field not found in the decrypted db object"
    private final val jsonUtils = JsonUtils()

    fun skipBadDecryptedRecords(item: SourceRecord, decrypted: String): DecryptedRecord? {
        val hbaseRowKey = Arrays.copyOfRange(item.hbaseRowId, 4, item.hbaseRowId.size)
        val hbaseRowId = String(hbaseRowKey)
        val db = item.db
        val collection = item.collection
        val type = item.innerType
        try {
            val dbObject = parseDecrypted(decrypted)
            if (null != dbObject) {
                val idElement = retrieveId(dbObject)
                val originalIdAsString = if (idElement is JsonObject) {
                    jsonUtils.sortJsonByKey(idElement.toString())
                } else {
                    idElement.asString
                }
                
                val (originalId, idWithWrappedDates: JsonElement) = if (idElement is JsonObject) {
                    Pair(idElement.toString(), wrapDates(idElement, false).first)
                }
                else {
                    Pair(idElement.asString, idElement)
                }

                val dbObjectWithId = if (idElement is JsonObject) {
                    dbObject.remove("_id")
                    dbObject.add("_id", idWithWrappedDates)
                    dbObject
                }
                else {
                    replaceElementValueWithKeyValuePair(dbObject, "_id", "\$oid", originalId)
                }

                val (dbObjectWithWrappedDates, lastModifiedDate) = wrapDates(dbObjectWithId)

                val newIdElement = dbObjectWithWrappedDates["_id"]
                val newIdAsString = if (newIdElement is JsonObject) {
                    jsonUtils.sortJsonByKey(newIdElement.toString())
                } else {
                    newIdElement.asString
                }

                val dateForManifest = getDateTimeForManifest(type, dbObjectWithId, lastModifiedDate)
                val timeAsLong = timestampAsLong(dateForManifest)
                val manifestRecord = ManifestRecord(newIdAsString,
                        timeAsLong, db, collection, "EXPORT", item.outerType, type, originalIdAsString)

                dbObjectWithWrappedDates.addProperty("timestamp", item.hbaseTimestamp)
                return DecryptedRecord(dbObjectWithWrappedDates, manifestRecord)
            }
        }
        catch (e: Exception) {
            e.printStackTrace(System.err)
            logError(logger, "Error decrypting record", e, "exceptionMessage", e.message
                    ?: "No message", "is_blank", "${StringUtils.isBlank(decrypted)}", "hbase_row_id", printableKey(item.hbaseRowId), "db_name", db, "collection_name", collection)
            throw BadDecryptedDataException(hbaseRowId, db, collection, e.message ?: "No exception message")
        }
        return null
    }

    fun wrapDates(objectWithDatesIn: JsonObject, useDateTimeSubstitute: Boolean = true): Pair<JsonObject, String> {
        val lastModifiedDateTimeAsString = retrieveLastModifiedDateTime(objectWithDatesIn, useDateTimeSubstitute)
        val dbObjectWithLastModifiedDate = if (StringUtils.isNotBlank(lastModifiedDateTimeAsString)) {
            val formattedLastModifiedDateTimeAsString = formatDateTimeToValidOutgoingFormat(lastModifiedDateTimeAsString)
            replaceElementValueWithKeyValuePair(
                    objectWithDatesIn,
                    LAST_MODIFIED_DATE_TIME_FIELD,
                    INNER_DATE_FIELD,
                    formattedLastModifiedDateTimeAsString)
        }
        else {
            objectWithDatesIn
        }

        val createdDateTimeAsString = retrieveDateTimeElement(CREATED_DATE_TIME_FIELD, objectWithDatesIn)
        val dbObjectWithLastModifiedAndCreatedDates = if (StringUtils.isNotBlank(createdDateTimeAsString)) {
            val formattedCreatedDateTimeAsString = formatDateTimeToValidOutgoingFormat(createdDateTimeAsString)
            replaceElementValueWithKeyValuePair(
                    dbObjectWithLastModifiedDate,
                    CREATED_DATE_TIME_FIELD,
                    INNER_DATE_FIELD,
                    formattedCreatedDateTimeAsString)
        }
        else {
            dbObjectWithLastModifiedDate
        }

        val removedDateTimeAsString = retrieveDateTimeElement(REMOVED_DATE_TIME_FIELD, objectWithDatesIn)
        val dbObjectWithLastModifiedCreatedAndRemovedDates = if (StringUtils.isNotBlank(removedDateTimeAsString)) { 
            val formattedRemovedDateTimeAsString = formatDateTimeToValidOutgoingFormat(removedDateTimeAsString)
            replaceElementValueWithKeyValuePair(
                dbObjectWithLastModifiedAndCreatedDates, 
                REMOVED_DATE_TIME_FIELD, 
                INNER_DATE_FIELD, 
                formattedRemovedDateTimeAsString) 
            } else { dbObjectWithLastModifiedAndCreatedDates }

        val archivedDateTimeAsString = retrieveDateTimeElement(ARCHIVED_DATE_TIME_FIELD, objectWithDatesIn)
        val dbObjectWithAllDates = if (StringUtils.isNotBlank(archivedDateTimeAsString)) { 
            val formattedArchivedDateTimeAsString = formatDateTimeToValidOutgoingFormat(archivedDateTimeAsString)
            replaceElementValueWithKeyValuePair(
                dbObjectWithLastModifiedCreatedAndRemovedDates, 
                ARCHIVED_DATE_TIME_FIELD, 
                INNER_DATE_FIELD, 
                formattedArchivedDateTimeAsString) 
            } else { dbObjectWithLastModifiedCreatedAndRemovedDates }

        return Pair(dbObjectWithAllDates, lastModifiedDateTimeAsString)
    }

    fun replaceElementValueWithKeyValuePair(objectWithFieldIn: JsonObject, keyToReplace: String, newKey: String, value: String): JsonObject {
        val objectWithChangedField = objectWithFieldIn
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

    fun retrieveLastModifiedDateTime(jsonObject: JsonObject, useSubstitute: Boolean = true): String {
        val epoch = "1980-01-01T00:00:00.000Z"
        val lastModifiedDateTime = retrieveDateTimeElement(LAST_MODIFIED_DATE_TIME_FIELD, jsonObject)
        val createdDateTime = retrieveDateTimeElement(CREATED_DATE_TIME_FIELD, jsonObject)

        if (StringUtils.isEmpty(lastModifiedDateTime) == false) {
            return lastModifiedDateTime
        }

        if (useSubstitute) {
            return if (StringUtils.isNotBlank(createdDateTime)) createdDateTime else epoch
        }

        return ""
    }

    fun getDateTimeForManifest(type: String, jsonObject: JsonObject, fallbackDate: String): String {
        if (type == MONGO_INSERT_TYPE) {
            val createdDateTimeAsString = retrieveDateTimeElement(CREATED_DATE_TIME_FIELD, jsonObject)
            if (StringUtils.isNotBlank(createdDateTimeAsString)) {
                return createdDateTimeAsString
            }
        }
        
        if (type == MONGO_DELETE_TYPE) {
            val removedDateTimeAsString = retrieveDateTimeElement(REMOVED_DATE_TIME_FIELD, jsonObject)
            if (StringUtils.isNotBlank(removedDateTimeAsString)) {
                return removedDateTimeAsString
            }
            val archivedDateTimeAsString = retrieveDateTimeElement(ARCHIVED_DATE_TIME_FIELD, jsonObject)
            if (StringUtils.isNotBlank(archivedDateTimeAsString)) {
                return archivedDateTimeAsString
            }
        }

        return fallbackDate
    }

    fun retrieveDateTimeElement(key: String, jsonObject: JsonObject): String {
        val dateElement = jsonObject[key]
        if (dateElement != null && dateElement.isJsonNull == false) {
            if (dateElement is JsonObject) {
                val dateSubElement = dateElement[INNER_DATE_FIELD]
                if (dateSubElement != null && dateSubElement.isJsonNull == false) {
                    return dateSubElement.asString
                }
            }
            else {
                return dateElement.asString
            }
        }

        return ""
    }

    fun formatDateTimeToValidOutgoingFormat(currentDateTime: String): String {
        val parsedDateTime = getValidParsedDateTime(currentDateTime)
        val df = SimpleDateFormat(validOutgoingFormat)
        return df.format(parsedDateTime)
    }

    fun getValidParsedDateTime(timestampAsString: String): Date {
        validTimestamps.forEach {
            try {
                val df = SimpleDateFormat(it)
                return df.parse(timestampAsString)
            } catch (e: Exception) {
                logDebug(logger, "timestampAsString did not match valid formats", "date_time_string", timestampAsString, "failed_format", it)
            }
        }
        throw Exception("Unparseable date found: '$timestampAsString', did not match any supported date formats")
    }

    fun timestampAsLong(timestampAsString: String): Long {
        val parsedDateTime = getValidParsedDateTime(timestampAsString)
        return parsedDateTime.time
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(Validator::class.toString())

        const val MONGO_INSERT_TYPE = "MONGO_INSERT"
        const val MONGO_DELETE_TYPE = "MONGO_DELETE"

        const val LAST_MODIFIED_DATE_TIME_FIELD = "_lastModifiedDateTime"
        const val CREATED_DATE_TIME_FIELD = "createdDateTime"
        const val REMOVED_DATE_TIME_FIELD = "_removedDateTime"
        const val ARCHIVED_DATE_TIME_FIELD = "_archivedDateTime"
        const val INNER_DATE_FIELD = "\$date"
    }
}
