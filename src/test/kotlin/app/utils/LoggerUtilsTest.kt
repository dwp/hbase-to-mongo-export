package app.utils

import com.nhaarman.mockitokotlin2.*
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner


@RunWith(SpringRunner::class)
@ActiveProfiles("aesCipherService", "httpDataKeyService", "unitTest", "outputToConsole")
@SpringBootTest
@TestPropertySource(properties = [
    "data.table.name=ucfs-data",
    "data.key.service.url=dummy.com:8090",
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
class LoggerUtilsTest {

    @Test
    fun testSemiFormattedTuples_willFormatAllTuples_WhenCalledWithoutMatchingKeyValuePairs() {
        assertEquals(
                "my-message",
                semiFormattedTuples("my-message"))
    }

    @Test
    fun testSemiFormattedTuples_willFormatAllTuples_WhenCalledWithMatchingKeyValuePairs() {
        assertEquals(
                "my-message\", \"key1\":\"value1\", \"key2\":\"value2",
                semiFormattedTuples("my-message", "key1", "value1", "key2", "value2"))
    }

    @Test
    fun testSemiFormattedTuples_willFailWithException_WhenCalledWithoutMatchingKeyValuePairs() {
        try {
            semiFormattedTuples("my-message", "key1")
            fail("Expected an IllegalArgumentException")
        } catch (expected: IllegalArgumentException) {
            assertEquals("Must have matched key-value pairs but had 1 argument(s)", expected.message)
        }
    }

    @Test
    fun testLoggerUtils_Debug_willFormatAllTuples_WhenCalled() {
        val mockLogger: org.slf4j.Logger = mock()

        logDebug(mockLogger, "main-message", "key1", "value1", "key2", "value2")

        verify(mockLogger, times(1)).debug("main-message\", \"key1\":\"value1\", \"key2\":\"value2")
        verifyNoMoreInteractions(mockLogger)
    }

    @Test
    fun testLoggerUtils_Info_willFormatAllTuples_WhenCalled() {
        val mockLogger: org.slf4j.Logger = mock()

        logInfo(mockLogger, "main-message", "key1", "value1", "key2", "value2")

        verify(mockLogger, times(1)).info("main-message\", \"key1\":\"value1\", \"key2\":\"value2")
        verifyNoMoreInteractions(mockLogger)
    }

    @Test
    fun testLoggerUtils_Error_willFormatAllTuples_WhenCalled() {
        val mockLogger: org.slf4j.Logger = mock()

        logError(mockLogger, "main-message", "key1", "value1", "key2", "value2")

        verify(mockLogger, times(1)).error("main-message\", \"key1\":\"value1\", \"key2\":\"value2")
        verifyNoMoreInteractions(mockLogger)
    }

    @Test
    fun testLoggerUtils_Error_willFormatAllTuples_WhenCalledWithKeyValuePairsAndException() {
        val mockLogger: org.slf4j.Logger = mock()
        val exception = RuntimeException("boom")

        logError(mockLogger, "main-message", exception, "key1", "value1", "key2", "value2")

        verify(mockLogger, times(1)).error(eq("main-message\", \"key1\":\"value1\", \"key2\":\"value2"), same(exception))
        verifyNoMoreInteractions(mockLogger)
    }

}
