package app.batch

import app.domain.DecryptedRecord
import app.domain.SourceRecord
import app.exceptions.DataKeyServiceUnavailableException
import app.exceptions.DecryptionFailureException
import app.services.CipherService
import app.services.KeyService
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
            return validator.skipBadDecryptedRecords(item, decrypted)
        } catch (e: DataKeyServiceUnavailableException) {
            dksNewDataKeyFailuresCounter.inc()
            throw e
        } catch (e: Exception) {
           throw DecryptionFailureException(item.hbaseRowId, item.encryption.keyEncryptionKeyId,e)
        }
    }

    companion object {
        val logger = DataworksLogger.getLogger(DecryptionProcessor::class)
    }
}





