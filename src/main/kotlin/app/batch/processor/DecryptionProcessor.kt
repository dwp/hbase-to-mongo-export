package app.batch.processor

import app.domain.DecryptedRecord
import app.domain.SourceRecord
import app.exceptions.DataKeyServiceUnavailableException
import app.exceptions.DecryptionFailureException
import app.services.CipherService
import app.services.KeyService
import com.google.gson.Gson
import com.google.gson.JsonObject
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

@Component
class DecryptionProcessor(private val cipherService: CipherService,
                          private val keyService: KeyService) :
        ItemProcessor<SourceRecord, DecryptedRecord> {

    @Throws(DataKeyServiceUnavailableException::class)
    override fun process(item: SourceRecord): DecryptedRecord? {
        try {
            logger.info("Processing '$item'.")
            val decryptedKey = keyService.decryptKey(
                    item.encryption.keyEncryptionKeyId,
                    item.encryption.encryptedEncryptionKey)
            val decrypted =
                    cipherService.decrypt(
                            decryptedKey,
                            item.encryption.initializationVector,
                            item.dbObject)
            val jsonObject = Gson().fromJson(decrypted, JsonObject::class.java)
            jsonObject.addProperty("timestamp", item.hbaseTimestamp)
            return DecryptedRecord(jsonObject, item.db, item.collection)
        } catch (e: DataKeyServiceUnavailableException) {
            throw e
        } catch (e: Exception) {
            logger.error("Rejecting '$item': '${e.message}': '${e.javaClass}': '${e.message}'.")
            throw DecryptionFailureException(
                    "database-unknown",
                    "collection-unknown",
                    item.hbaseRowId,
                    item.hbaseTimestamp,
                    item.encryption.keyEncryptionKeyId,
                    e)
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DecryptionProcessor::class.toString())
    }

}