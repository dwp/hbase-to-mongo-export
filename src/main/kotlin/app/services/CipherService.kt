package app.services

import app.domain.EncryptionResult

interface CipherService {
    fun decrypt(key: String, initializationVector: String, encrypted: String): String
    fun encrypt(key: String, unencrypted: ByteArray): EncryptionResult
}