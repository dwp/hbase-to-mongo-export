package app.services.impl

import app.services.DecryptionService
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("phoneyDecryptionService")
class PhoneyDecryptionService: DecryptionService {
    override fun decrypt(key: String, initializationVector: String, encrypted: String) =
            """{ "decryptedObject": "${encrypted.substring(10)} ..." }"""
}