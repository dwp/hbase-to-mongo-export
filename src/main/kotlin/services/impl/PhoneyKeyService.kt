package app.services.impl

import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import app.services.KeyService

@Service
@Profile("phoneyServices")
class PhoneyKeyService: KeyService {
    override fun decryptKey(encryptionKeyId: String, encryptedKey: String) =
            "[ DECRYPTED VERSION OF '$encryptedKey', DECRYPTED WITH '$encryptionKeyId' ]"
}