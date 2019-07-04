package uk.gov.dwp.dataworks.export.services

interface DecryptionService {
    fun decrypt(key: String, data: String): String
}