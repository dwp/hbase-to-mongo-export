package app.services.impl

import app.domain.EncryptionResult
import app.services.CipherService
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import java.io.OutputStream
import java.security.Key
import javax.crypto.Cipher

@Service
@Profile("phoneyCipherService")
class PhoneyCipherService : CipherService {

    override fun encrypt(key: String, unencrypted: ByteArray): EncryptionResult {
        return EncryptionResult(initialisationVector = "dummy-vector", encrypted = "dummy-encrypted-data")
    }

    override fun decryptingCipher(key: Key, initialisationVector: ByteArray): Cipher {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun encryptingCipher(key: Key, initialisationVector: ByteArray): Cipher {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun cipherOutputStream(key: Key, initialisationVector: ByteArray, target: OutputStream): OutputStream {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun decrypt(key: String, initializationVector: String, encrypted: String) =
        """{ "decryptedObject": "${encrypted.substring(10)} ..." }"""
}