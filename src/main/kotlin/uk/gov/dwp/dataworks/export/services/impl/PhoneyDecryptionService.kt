package uk.gov.dwp.dataworks.export.services.impl

import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.export.services.DecryptionService

@Service
@Profile("phoneyServices")
class PhoneyDecryptionService: DecryptionService {
    override fun decrypt(key: String, data: String) =
            "[ DECRYPTED VERSION OF '${data.substring(0, 10)} ...' ]"
}