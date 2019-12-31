package app.services.impl

import app.configuration.HttpClientProvider
import app.domain.DataKeyResult
import app.exceptions.DataKeyDecryptionException
import app.exceptions.DataKeyServiceUnavailableException
import app.services.KeyService
import app.utils.logging.logDebug
import app.utils.logging.logWarn
import com.google.gson.Gson
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URLEncoder

@Service
@Profile("httpDataKeyService")
class HttpKeyService(private val httpClientProvider: HttpClientProvider) : KeyService {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HttpKeyService::class.toString())

        // Will retry at 1s, 2s, 4s, 8s, 16s then give up (after a total of 31 secs)
        const val maxAttempts = 5
        const val initialBackoffMillis = 1000L
        const val backoffMultiplier = 2.0
    }

    @Override
    @Retryable(value = [DataKeyServiceUnavailableException::class],
        maxAttempts = maxAttempts,
        backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    @Throws(DataKeyServiceUnavailableException::class)
    override fun batchDataKey(): DataKeyResult {
        try {
            val dksUrl = "$dataKeyServiceUrl/datakey"
            httpClientProvider.client().use { client ->
                client.execute(HttpGet(dksUrl)).use { response ->
                    val statusCode = response.statusLine.statusCode
                    return if (statusCode == 201) {
                        val entity = response.entity
                        val result = BufferedReader(InputStreamReader(entity.content))
                            .use(BufferedReader::readText).let {
                                Gson().fromJson(it, DataKeyResult::class.java)
                            }
                        EntityUtils.consume(entity)
                        result
                    }
                    else {
                        logWarn(logger, "dataKeyServiceUrl: '$dksUrl' returned status code '$statusCode'.")
                        throw DataKeyServiceUnavailableException("data key service returned status code '$statusCode'.")
                    }
                }
            }
        }
        catch (ex: Exception) {
            when(ex) {
                is DataKeyServiceUnavailableException -> {
                    throw ex
                }
                else -> throw DataKeyServiceUnavailableException("Error contacting data key service: $ex")
            }
        }
    }

    @Override
    @Retryable(value = [DataKeyServiceUnavailableException::class],
        maxAttempts = maxAttempts,
        backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    @Throws(DataKeyServiceUnavailableException::class, DataKeyDecryptionException::class)
    override fun decryptKey(encryptionKeyId: String, encryptedKey: String): String {
        try {
            val cacheKey = "$encryptedKey/$encryptionKeyId"
            return if (decryptedKeyCache.containsKey(cacheKey)) {
                decryptedKeyCache[cacheKey]!!
            }
            else {
                httpClientProvider.client().use { client ->
                    val dksUrl = """$dataKeyServiceUrl/datakey/actions/decrypt?keyId=${URLEncoder.encode(encryptionKeyId, "US-ASCII")}"""
                    logDebug(logger, "Calling dataKeyServiceUrl: '$dksUrl'.")
                    val httpPost = HttpPost(dksUrl)
                    httpPost.entity = StringEntity(encryptedKey, ContentType.TEXT_PLAIN)
                    client.execute(httpPost).use { response ->
                        val statusCode = response.statusLine.statusCode
                        logDebug(logger, "dataKeyServiceUrl: '$dksUrl' returned status code '$statusCode'.")
                        return when (statusCode) {
                            200 -> {
                                val entity = response.entity
                                val text = BufferedReader(InputStreamReader(response.entity.content)).use(BufferedReader::readText)
                                EntityUtils.consume(entity)
                                val dataKeyResult = Gson().fromJson(text, DataKeyResult::class.java)
                                decryptedKeyCache[cacheKey] = dataKeyResult.plaintextDataKey
                                dataKeyResult.plaintextDataKey
                            }
                            400 ->
                                throw DataKeyDecryptionException(
                                    "Decrypting encryptedKey: '$encryptedKey' with keyEncryptionKeyId: '$encryptionKeyId' data key service returned status code '$statusCode'")
                            else ->
                                throw DataKeyServiceUnavailableException(
                                    "Decrypting encryptedKey: '$encryptedKey' with keyEncryptionKeyId: '$encryptionKeyId' data key service returned status code '$statusCode'")
                        }
                    }
                }
            }
        }
        catch (ex: Exception) {
            when(ex) {
                is DataKeyDecryptionException, is DataKeyServiceUnavailableException -> {
                    throw ex
                }
                else -> throw DataKeyServiceUnavailableException("Error contacting data key service: $ex")
            }
        }
    }

    fun clearCache() {
        this.decryptedKeyCache = mutableMapOf()
    }

    private var decryptedKeyCache = mutableMapOf<String, String>()

    @Value("\${data.key.service.url}")
    private lateinit var dataKeyServiceUrl: String
}
