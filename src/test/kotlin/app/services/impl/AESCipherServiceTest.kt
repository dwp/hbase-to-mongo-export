package app.services.impl

import app.services.CipherService
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.junit4.SpringRunner
import java.security.SecureRandom

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [AESCipherService::class])
class AESCipherServiceTest {

    @Test
    fun testEncryptionDecryption() {
        val key = "czMQLgW/OrzBZwFV9u4EBA=="
        val original = "Original unencrypted text that should come out of decrypt"
        val (initialisationVector, encrypted) = cipherService.encrypt(key, original.toByteArray())
        val decrypted = cipherService.decrypt(key, initialisationVector, encrypted)
        assertEquals(original, decrypted)
    }

    @Test
    fun testWrongKey() {
        val key = "czMQLgW/OrzBZwFV9u4EBA=="
        val original = "Original unencrypted text that should come out of decrypt."
        val (initialisationVector, encrypted) = cipherService.encrypt(key, original.toByteArray())
        val decrypted = cipherService.decrypt(key.replace('c', 'd'), initialisationVector, encrypted)
        assertNotEquals(original, decrypted)
    }

    // This test is flaky - intended to show that changing the IV changes the decryption result.
    // Usually it does but sometimes it doesn't. Need to check out the strategy being used to change the IV.
    fun testWrongIv() {
        val key = "czMQLgW/OrzBZwFV9u4EBA=="
        val original = "Original unencrypted text that should come out of decrypt."
        val (initialisationVector, encrypted) = cipherService.encrypt(key, original.toByteArray())
        val firstChar = initialisationVector[0]
        val decrypted =
            cipherService.decrypt(key, initialisationVector.replace(firstChar, firstChar + 1), encrypted)
        assertNotEquals(original, decrypted)
    }

    @Autowired
    private lateinit var cipherService: CipherService

    @MockBean
    private lateinit var secureRandom: SecureRandom
}
