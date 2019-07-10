package app.services

interface DecryptionService {
    fun decrypt(key: String, data: String): String
}