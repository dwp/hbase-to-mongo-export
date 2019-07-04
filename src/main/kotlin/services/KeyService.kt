package app.services

interface KeyService {
    fun decryptKey(encryptionKeyId: String, encryptedKey: String): String
}