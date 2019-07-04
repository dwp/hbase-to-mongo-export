package uk.gov.dwp.dataworks.export.services.impl

import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.export.services.KeyService

@Service
@Profile("phoneyServices")
class PhoneyKeyService: KeyService {
    override fun decryptKey(encryptionKeyId: String, encryptedKey: String) =
            "[ DECRYPTED VERSION OF '$encryptedKey', DECRYPTED WITH '$encryptionKeyId' ]"
}