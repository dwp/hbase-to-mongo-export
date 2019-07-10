package app.batch

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import app.domain.SourceRecord
import app.services.DecryptionService
import app.services.KeyService
import com.google.gson.Gson

@Component
class DecryptionProcessor(private val decryptionService: DecryptionService,
                          private val keyService: KeyService):
        ItemProcessor<SourceRecord, String> {

    override fun process(item: SourceRecord): String? {
        logger.info("item: '$item'.")

        val decryptedKey: String =
                keyService.decryptKey(item.encryption.encryptionKeyId,
                        item.encryption.encryptedEncryptionKey)

        val decryptedRecord: String =
                decryptionService.decrypt(decryptedKey, item.dbObject)

        item.dbObject= decryptedRecord
        return Gson().toJson(item)

    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DecryptionProcessor::class.toString())
    }

}