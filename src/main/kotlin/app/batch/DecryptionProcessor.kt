package app.batch

import app.domain.DecryptedRecord
import app.domain.SourceRecord
import app.exceptions.DataKeyServiceUnavailableException
import app.exceptions.DecryptionFailureException
import app.services.CipherService
import app.services.KeyService
import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import io.prometheus.client.Counter
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Component
class DecryptionProcessor(private val cipherService: CipherService,
                          private val keyService: KeyService,
                          private val validator: Validator,
                          private val dksNewDataKeyFailuresCounter: Counter) :
    ItemProcessor<SourceRecord, DecryptedRecord> {

    private final val BUSINESS_AUDIT_DB = "data"
    private final val BUSINESS_AUDIT_COLLECTION = "businessAudit"
    private final val KEY_CONTEXT = "context"
    private final val KEY_AUDIT_EVENT = "AUDIT_EVENT"
    private final val KEY_TIME_STAMP = "TIME_STAMP"
    private final val KEY_TIME_STAMP_ORIG = "TIME_STAMP_ORIG"
    private final val KEY_AUDIT_TYPE = "auditType"

    private val gson = GsonBuilder().serializeNulls().create()

    @Throws(DataKeyServiceUnavailableException::class)
    override fun process(item: SourceRecord): DecryptedRecord? {
        try {
            val decryptedKey = keyService.decryptKey(
                item.encryption.keyEncryptionKeyId,
                item.encryption.encryptedEncryptionKey)
            val decrypted =
                cipherService.decrypt(
                    decryptedKey,
                    item.encryption.initializationVector,
                    item.dbObject)
            val db = item.db
            val collection = item.collection
            logger.debug("db: $db, collection: $collection")
            if(db == BUSINESS_AUDIT_DB && collection == BUSINESS_AUDIT_COLLECTION) {
                transform(item, decrypted)
            }
            return validator.skipBadDecryptedRecords(item, decrypted)
        } catch (e: DataKeyServiceUnavailableException) {
            dksNewDataKeyFailuresCounter.inc()
            throw e
        } catch (e: Exception) {
           throw DecryptionFailureException(item.hbaseRowId, item.encryption.keyEncryptionKeyId,e)
        }
    }

    fun transform(item: SourceRecord, decrypted: String): String {
        logger.debug("Processing business audit record ${item.hbaseRowId}")
        val dbObject = gson.fromJson(decrypted, JsonObject::class.java)
        val contextElement: JsonObject = dbObject[KEY_CONTEXT] as JsonObject
        val auditType = dbObject[KEY_AUDIT_TYPE]
        if ((auditType != null && !auditType.isJsonNull) &&
                contextElement != null && !contextElement.isJsonNull) {
            contextElement.addProperty(KEY_AUDIT_EVENT, auditType.asString)
            contextElement.addProperty(KEY_TIME_STAMP, item.messageLastModifiedDateTime)
            contextElement.addProperty(KEY_TIME_STAMP_ORIG, item.messageLastModifiedDateTime)
            return gson.toJson(contextElement)
        } else {
            throw Exception("auditType or context for business audit record is null")
        }
    }

    companion object {
        val logger = DataworksLogger.getLogger(DecryptionProcessor::class)
    }
}





