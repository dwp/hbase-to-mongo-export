package app.batch

import app.batch.processor.DecryptionProcessor
import app.domain.DecryptedRecord
import app.domain.SourceRecord
import app.exceptions.BadDecryptedDataException
import com.google.gson.Gson
import com.google.gson.JsonObject
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.text.SimpleDateFormat
import java.util.*

@Component
class Validator {

    fun skipBadDecryptedRecords(item: SourceRecord, decrypted: String): DecryptedRecord? {
        val hbaseRowKey = Arrays.copyOfRange(item.hbaseRowId, 4, item.hbaseRowId.size)
        val hbaseRowId = String(hbaseRowKey)
        val db = item.db
        val collection = item.collection
        try {
            val jsonObject = parseDecrypted(hbaseRowId, decrypted)
            if (null != jsonObject) {
                retrieveId(hbaseRowId, jsonObject)
                val lastUpdatedTimestamp = retrievelastUpdatedTimestamp(hbaseRowId, jsonObject)
                lastUpdatedTimestamp?.let { validate(hbaseRowId, lastUpdatedTimestamp) }
                jsonObject.addProperty("timestamp", item.hbaseTimestamp)
                return DecryptedRecord(jsonObject, db, collection)
            }
        } catch (e: Exception) {
            val ex = BadDecryptedDataException(hbaseRowId, db, collection, e.message!!)
            DecryptionProcessor.logger.error(ex.message)
            throw ex
        }
        return null
    }

    fun parseDecrypted(hbaseRowId: String, decrypted: String): JsonObject? {
        try {
            val jsonObject = Gson().fromJson(decrypted, JsonObject::class.java)
            return jsonObject
        } catch (e: Exception) {
            val parsingException = "Exception occurred while parsing decrypted db object"
            throw Exception(parsingException)
        }
        return null
    }

    fun retrieveId(hbaseRowId: String, jsonObject: JsonObject): JsonObject? {
        try {
            val id = jsonObject.getAsJsonObject("_id")
            if (null == id) {
                val idNotFound = "id not found in the decrypted db object"
                throw Exception(idNotFound)
            }
            return id
        } catch (e: Exception) {
            val idNotFoundException = "Exception : ${e.message}"
            throw Exception(idNotFoundException)
        }
        return null
    }

    fun retrievelastUpdatedTimestamp(hbaseRowId: String, jsonObject: JsonObject): JsonObject? {
        try {
            val lastUpdatedTimestamp = jsonObject.getAsJsonObject("_lastModifiedDateTime")
            if (null == lastUpdatedTimestamp) {
                val _lastModifiedDateTimeNotFound = "_lastModifiedDateTime not found in the decrypted db object"
                throw Exception(_lastModifiedDateTimeNotFound)
            }
            return lastUpdatedTimestamp
        } catch (e: Exception) {
            val _lastModifiedDateTimeException = "Exception : ${e.message}"
            throw Exception(_lastModifiedDateTimeException)
        }
        return null
    }

    fun validate(hbaseRowId: String, lastUpdatedTimestamp: JsonObject): Long? {
        val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ")
        try {
            val date = lastUpdatedTimestamp.getAsJsonPrimitive("\$date")
            if (null != date) {
                return df.parse(date.toString()).time
            } else {
                val dateNotFound = "\$date in _lastModifiedDateTime not found in the decrypted db object"
                throw Exception(dateNotFound)
            }
        } catch (e: Exception) {
            val formatException = "Exception : ${e.message}"
            throw Exception(formatException)
        }
        return null
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(Validator::class.toString())
    }
}





