package app.services

import app.domain.EncryptionResult
import java.io.OutputStream
import java.security.Key
import javax.crypto.Cipher

interface CipherService {
    fun decrypt(key: String, initializationVector: String, encrypted: String): String
    fun encrypt(key: String, unencrypted: ByteArray): EncryptionResult
    fun decryptingCipher(key: Key, initialisationVector: ByteArray): Cipher
    fun encryptingCipher(key: Key, initialisationVector: ByteArray): Cipher
    fun cipherOutputStream(key: Key, initialisationVector: ByteArray, target: OutputStream): OutputStream
}
