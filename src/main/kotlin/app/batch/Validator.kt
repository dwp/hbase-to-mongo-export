package app.batch

import app.domain.DecryptedRecord
import app.domain.ManifestRecord
import app.domain.SourceRecord
import app.exceptions.BadDecryptedDataException
import app.utils.logging.logDebug
import app.utils.logging.logError
import com.google.gson.Gson
import com.google.gson.JsonObject
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.text.SimpleDateFormat
import java.util.*

@Component
class Validator {
    val defaultType = "TYPE_NOT_SET"
    val validTimestamps = listOf("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val idNotFound = "_id field not found in the decrypted db object"
    val parsingException = "Exception occurred while parsing decrypted db object"

    fun skipBadDecryptedRecords(item: SourceRecord, decrypted: String): DecryptedRecord? {
        val hbaseRowKey = Arrays.copyOfRange(item.hbaseRowId, 4, item.hbaseRowId.size)
        val hbaseRowId = String(hbaseRowKey)
        val db = item.db
        val collection = item.collection
        try {
            val jsonObject = parseDecrypted(decrypted)
            logDebug(logger, "Successfully parsed decrypted object")
            if (null != jsonObject) {
                val id = retrieveId(jsonObject)
                val timeAsLong = timestampAsLong(item.lastModified)
                jsonObject.addProperty("timestamp", item.hbaseTimestamp)
                val externalSource = retrieveType(jsonObject)
                val manifestRecord = ManifestRecord(id!!.toString(), timeAsLong, db, collection, "EXPORT", externalSource)
                return DecryptedRecord(jsonObject, manifestRecord)
            }
        } catch (e: Exception) {
            val ex = BadDecryptedDataException(hbaseRowId, db, collection, e.message!!)
            logError(logger, ex.message!!)
            throw ex
        }
        return null
    }

    fun parseDecrypted(decrypted: String): JsonObject? {
        try {
            return Gson().fromJson(decrypted, JsonObject::class.java)
        } catch (e: Exception) {
            throw Exception(parsingException)
        }
    }

    fun retrieveId(jsonObject: JsonObject): JsonObject? {
        return jsonObject.getAsJsonObject("_id") ?: throw Exception(idNotFound)
    }


    fun timestampAsLong(lastUpdatedTimestamp: String): Long {
        validTimestamps.forEach {
            try {
                val df = SimpleDateFormat(it)
                return df.parse(lastUpdatedTimestamp).time
            }
            catch (e: Exception) {
                logDebug(logger, "lastUpdatedTimestamp did not match valid formats", "valid_formats", "$validTimestamps")
            }
        }
        throw Exception("Unparseable date found: \"$lastUpdatedTimestamp\"")
    }

    fun retrieveType(jsonObject: JsonObject): String {
        val typeElement = jsonObject.get("@type")
        logDebug(logger, "Getting @type field", "type_field", "$typeElement")

        if (typeElement != null) {
            return typeElement.asString
        }
        return defaultType
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(Validator::class.toString())
    }
}
