package app.batch

import app.domain.DecryptedRecord
import app.domain.ManifestRecord
import app.domain.SourceRecord
import app.exceptions.BadDecryptedDataException
import app.utils.DateWrapper
import app.utils.JsonUtils
import com.google.gson.GsonBuilder
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import org.apache.commons.lang3.StringUtils
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*

@Component
class Validator {
    private final val validIncomingFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"
    private final val validOutgoingFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

    val validTimestamps = listOf(validIncomingFormat, validOutgoingFormat)
    val idNotFound = "_id field not found in the decrypted db object"
    private final val jsonUtils = JsonUtils()
    private val gson = GsonBuilder().serializeNulls().create()

    fun skipBadDecryptedRecords(item: SourceRecord, decrypted: String): DecryptedRecord? {
        val hbaseRowKey = Arrays.copyOfRange(item.hbaseRowId, 4, item.hbaseRowId.size)
        val hbaseRowId = String(hbaseRowKey)
        val db = item.db
        val collection = item.collection
        val type = item.innerType
        try {
            val dbObject = gson.fromJson(decrypted, JsonObject::class.java)
            if (dbObject != null) {
                val (dbObjectWithWrappedDates, lastModifiedDate) = wrapDates(dbObject)
                val idElement = retrieveId(dbObject)
                if (idElement.isJsonPrimitive) {
                    replaceElementValueWithKeyValuePair(dbObject, "_id", "\$oid", idElement.asJsonPrimitive.asString)
                }
                val newIdElement = dbObjectWithWrappedDates["_id"]
                val dateForManifest = getDateTimeForManifest(type, dbObjectWithWrappedDates, lastModifiedDate)
                val timeAsLong = timestampAsLong(dateForManifest, lastModifiedDate, snapshotType)
                val manifestRecord = ManifestRecord(elementAsString(newIdElement),
                        timeAsLong, db, collection, "EXPORT", item.outerType, type, elementAsString(idElement))
                return DecryptedRecord(dbObjectWithWrappedDates, manifestRecord)
            }
        }
        catch (e: Exception) {
            logger.error("Error decrypting record", e, "exceptionMessage" to (e.message ?: "No message"),
                    "is_blank" to "${StringUtils.isBlank(decrypted)}", "hbase_row_id" to printableKey(item.hbaseRowId), "db_name" to db, "collection_name" to collection)
            throw BadDecryptedDataException(hbaseRowId, db, collection, e.message ?: "No exception message")
        }
        return null
    }

    private fun elementAsString(idElement: JsonElement) =
        if (idElement is JsonObject) {
            jsonUtils.sortJsonByKey(idElement.toString())
        }
        else {
            idElement.asString
        }

    fun wrapDates(objectWithDatesIn: JsonObject): Pair<JsonObject, String> {
        val lastModifiedDateTimeAsString = retrieveLastModifiedDateTime(objectWithDatesIn)
        val dbObject = if (StringUtils.isNotBlank(lastModifiedDateTimeAsString)) {
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

        DateWrapper().processJsonObject(dbObject, false)
        return Pair(dbObject, lastModifiedDateTimeAsString)
    }

    fun replaceElementValueWithKeyValuePair(objectWithFieldIn: JsonObject, keyToReplace: String, newKey: String, value: String): JsonObject {
        val newElement = JsonObject()
        newElement.addProperty(newKey, value)
        objectWithFieldIn.remove(keyToReplace)
        objectWithFieldIn.add(keyToReplace, newElement)
        return objectWithFieldIn
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
        val lastModifiedDateTime = retrieveDateTimeElement(LAST_MODIFIED_DATE_TIME_FIELD, jsonObject)
        val createdDateTime = retrieveDateTimeElement(CREATED_DATE_TIME_FIELD, jsonObject)

        return when {
            !StringUtils.isEmpty(lastModifiedDateTime) -> {
                lastModifiedDateTime
            }
            StringUtils.isNotBlank(createdDateTime) -> {
                createdDateTime
            }
            else -> {
                epoch
            }
        }
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
        if (dateElement != null && !dateElement.isJsonNull) {
            if (dateElement is JsonObject) {
                val dateSubElement = dateElement[INNER_DATE_FIELD]
                if (dateSubElement != null && !dateSubElement.isJsonNull) {
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

    @Throws(ParseException::class)
    fun getValidParsedDateTime(timestampAsString: String): Date {
        validTimestamps.forEach {
            try {
                val df = SimpleDateFormat(it)
                return df.parse(timestampAsString)
            } catch (e: Exception) {
                logger.debug("timestampAsString did not match valid formats", "date_time_string" to timestampAsString, "failed_format" to it)
            }
        }
        throw ParseException("Unparseable date found: '$timestampAsString', did not match any supported date formats", 0)
    }

    fun timestampAsLong(createdDateTime: String, lastModifiedDateTime: String, snapshotTypeInUse): Long {
        val (manifestDateTimePreferred, fallbackDateTime) =
            if (snapshotTypeInUse == "full") {
                Pair(createdDateTime, lastModifiedDateTime)
            }
            else {
                Pair(lastModifiedDateTime, createdDateTime)
            }

        try {
            val parsedDateTime = getValidParsedDateTime(manifestDateTimePreferred)
            return parsedDateTime.time
        }
        catch (ex: ParseException) {
            logger.debug("Timestamp for manifest could not be parsed, so falling back to fallback", 
                "preferred_date_time" to manifestDateTimePreferred, 
                "last_modified_date_time" to fallbackDateTime,
                "snapshot_type" to snapshotType)

            val parsedDateTime = getValidParsedDateTime(fallbackDateTime)
            return parsedDateTime.time
        }
    }

    @Value("\${snapshot.type}")
    private lateinit var snapshotType: String

    companion object {
        val logger = DataworksLogger.getLogger(Validator::class.toString())

        const val MONGO_INSERT_TYPE = "MONGO_INSERT"
        const val MONGO_DELETE_TYPE = "MONGO_DELETE"

        const val LAST_MODIFIED_DATE_TIME_FIELD = "_lastModifiedDateTime"
        const val CREATED_DATE_TIME_FIELD = "createdDateTime"
        const val REMOVED_DATE_TIME_FIELD = "_removedDateTime"
        const val ARCHIVED_DATE_TIME_FIELD = "_archivedDateTime"
        const val INNER_DATE_FIELD = "\$date"
    }
}
