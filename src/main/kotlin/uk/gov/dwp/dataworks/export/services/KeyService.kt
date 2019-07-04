package uk.gov.dwp.dataworks.export.services

interface KeyService {
    fun decryptKey(encryptionKeyId: String, encryptedKey: String): String
}