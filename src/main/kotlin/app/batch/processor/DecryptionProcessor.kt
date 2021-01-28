package app.batch.processor

import app.batch.Validator
import app.domain.DecryptedRecord
import app.domain.SourceRecord
import app.exceptions.DataKeyServiceUnavailableException
import app.exceptions.DecryptionFailureException
import app.services.CipherService
import app.services.KeyService
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Component
class DecryptionProcessor(private val cipherService: CipherService,
                          private val keyService: KeyService, private val validator: Validator) :
    ItemProcessor<SourceRecord, DecryptedRecord> {

    @Throws(DataKeyServiceUnavailableException::class)
    override fun process(item: SourceRecord): DecryptedRecord? {
        try {
            logger.debug("Processing next item", "item" to "$item")
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
            logger.error("Rejecting invalid item", e, "item" to "$item")
            throw DecryptionFailureException(
                "database-unknown",
                "collection-unknown",
                item.hbaseRowId,
                item.encryption.keyEncryptionKeyId,
                e)
        }
    }

    companion object {
        val logger = DataworksLogger.getLogger(DecryptionProcessor::class)
    }
}





