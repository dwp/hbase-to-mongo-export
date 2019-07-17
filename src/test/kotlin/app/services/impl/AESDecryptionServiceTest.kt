package app.services.impl

import app.services.DecryptionService
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.security.Key
import java.util.*
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

@RunWith(SpringRunner::class)
@ActiveProfiles("aesDecryptionService", "phoneyDataKeyService", "unitTest", "outputToConsole")
@SpringBootTest
@TestPropertySource(properties = ["source.table.name=ucdata"])
class AESDecryptionServiceTest {

    @Test
    fun testDecrypts() {
        val iv = "0123456789ABCD"
        val key = "czMQLgW/OrzBZwFV9u4EBA=="
        val original = "Original unencrypted text that should come out of decrypt."
        val encrypted = encrypt(key, iv.toByteArray(), original)
        val decrypted = decryptionService.decrypt(key, iv, encrypted)
        logger.info("decrypted: '$decrypted'.")
        assertEquals(original, decrypted)
    }

    private fun encrypt(key: String, initializationVector: ByteArray, toEncrypt: String): String {
        var keySpec: Key = SecretKeySpec(key.toByteArray(), "AES")
        val cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC")
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, IvParameterSpec(Base64.getDecoder().decode(initializationVector)))
        val encrypted = cipher.doFinal(toEncrypt.toByteArray())
        val encryptedValue = Base64.getEncoder().encode(encrypted)
        return String(encryptedValue)
    }

//    fun decrypt(key: String, initializationVector: ByteArray, encrypted: String): String {
//        var keySpec: Key = SecretKeySpec(key.toByteArray(), "AES")
//        val cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC")
//        cipher.init(Cipher.DECRYPT_MODE, keySpec, IvParameterSpec(Base64.getDecoder().decode(initializationVector)))
//        val decodedBytes = Base64.getDecoder().decode(encrypted.toByteArray())
//        val original = cipher.doFinal(decodedBytes)
//        return String(original)
//    }

    @Autowired
    private lateinit var decryptionService: DecryptionService

    companion object {
        val logger: Logger = LoggerFactory.getLogger(AESDecryptionServiceTest::class.toString())
    }

}