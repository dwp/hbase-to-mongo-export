package app.services.impl

import app.services.KeyService
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("phoneyDataKeyService")
class PhoneyKeyService: KeyService {
    override fun decryptKey(encryptionKeyId: String, encryptedKey: String) = "czMQLgW/OrzBZwFV9u4EBA=="
}