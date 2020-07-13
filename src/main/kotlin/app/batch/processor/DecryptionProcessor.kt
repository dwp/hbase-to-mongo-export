package app.batch.processor

import app.batch.Validator
import app.domain.DecryptedRecord
import app.domain.SourceRecord
import app.exceptions.DataKeyServiceUnavailableException
import app.exceptions.DecryptionFailureException
import app.services.CipherService
import app.services.KeyService
import app.utils.logging.logDebug
import app.utils.logging.logError
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

@Component
class DecryptionProcessor(private val cipherService: CipherService,
                          private val keyService: KeyService, private val validator: Validator) :
    ItemProcessor<SourceRecord, DecryptedRecord> {

    @Throws(DataKeyServiceUnavailableException::class)
    override fun process(item: SourceRecord): DecryptedRecord? {
        try {
            logDebug(logger, "Processing next item", "item", "$item")
            val decryptedKey = keyService.decryptKey(
                item.encryption.keyEncryptionKeyId,
                item.encryption.encryptedEncryptionKey)
            val decrypted =
                cipherService.decrypt(
                    decryptedKey,
                    item.encryption.initializationVector,
                    item.dbObject)
            return validator.skipBadDecryptedRecords(item, decrypted)
        } catch (e: DataKeyServiceUnavailableException) {
            throw e
        } catch (e: Exception) {
            logError(logger, "Rejecting invalid item", e, "item", "$item")
            throw DecryptionFailureException(
                "database-unknown",
                "collection-unknown",
                item.hbaseRowId,
                item.encryption.keyEncryptionKeyId,
                e)
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DecryptionProcessor::class.toString())
    }
}





