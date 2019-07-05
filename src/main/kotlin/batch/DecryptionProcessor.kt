package app.batch

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import app.domain.SourceRecord
import app.services.DecryptionService
import app.services.KeyService

@Component
class DecryptionProcessor(private val decryptionService: DecryptionService,
                          private val keyService: KeyService):
        ItemProcessor<SourceRecord, SourceRecord> {

    override fun process(item: SourceRecord): SourceRecord? {
        logger.info("item: '$item'.")

        val decryptedKey: String =
                keyService.decryptKey(item.encryption.encryptionKeyId,
                        item.encryption.encryptedEncryptionKey)

        val decryptedRecord: String =
                decryptionService.decrypt(decryptedKey, item.dbObject)

        item.dbObject= decryptedRecord
        return item
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DecryptionProcessor::class.toString())
    }

}