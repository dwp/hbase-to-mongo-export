package app.services.impl

import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import app.services.DecryptionService

@Service
@Profile("phoneyServices")
class PhoneyDecryptionService: DecryptionService {
    override fun decrypt(key: String, data: String) =
            "[ DECRYPTED VERSION OF '${data.substring(0, 10)} ...' ]"
}