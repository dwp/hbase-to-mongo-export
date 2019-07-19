package app.services

interface DecryptionService {
    fun decrypt(key: String, initializationVector: String, encrypted: String): String
}