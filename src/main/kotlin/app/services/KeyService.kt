package app.services

import app.domain.DataKeyResult
import app.exceptions.DataKeyDecryptionException
import app.exceptions.DataKeyServiceUnavailableException
import app.services.impl.HttpKeyService
import org.slf4j.Logger
import org.slf4j.LoggerFactory

interface KeyService {

    @Throws(DataKeyServiceUnavailableException::class, DataKeyDecryptionException::class)
    fun decryptKey(encryptionKeyId: String, encryptedKey: String): String

    @Throws(DataKeyServiceUnavailableException::class)
    fun batchDataKey(): DataKeyResult
}