package app.services.impl

import app.domain.EncryptionResult
import app.services.CipherService
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("phoneyCipherService")
class PhoneyCipherService: CipherService {
//    override fun encryptOpenSsl(key: String, unencrypted: ByteArray): EncryptionResult {
//        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
//    }

//    override fun decryptCBC(key: String, initializationVector: String, encrypted: String): String {
//        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
//    }

    override fun encrypt(key: String, unencrypted: ByteArray): EncryptionResult {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun decrypt(key: String, initializationVector: String, encrypted: String) =
            """{ "decryptedObject": "${encrypted.substring(10)} ..." }"""
}