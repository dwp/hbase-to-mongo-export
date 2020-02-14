package app.services.impl

import app.services.CipherService
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@ActiveProfiles("aesCipherService", "phoneyDataKeyService", "unitTest", "outputToConsole")
@SpringBootTest
@TestPropertySource(properties = [
    "data.table.name=ucfs-data",
    "column.family=topic",
    "topic.name=db.a.b",
    "identity.keystore=resources/identity.jks",
    "trust.keystore=resources/truststore.jks",
    "identity.store.password=changeit",
    "identity.key.password=changeit",
    "trust.store.password=changeit",
    "identity.store.alias=cid",
    "hbase.zookeeper.quorum=hbase",
    "aws.region=eu-west-2"
])
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

    companion object {
    }

}
