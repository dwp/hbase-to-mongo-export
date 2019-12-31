package app.utils.logging

/**
Please see notes in the file under test (LoggerUtils) and it's class LoggerLayoutAppender.
 */

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
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
    fun testFormattedTimestamp_WillUseDefaultFormat() {
        assertEquals("01:00:00.000", formattedTimestamp(0))
        assertEquals("08:29:03.210", formattedTimestamp(9876543210))
        assertEquals("07:12:55.807", formattedTimestamp(Long.MAX_VALUE))
    }

    @Test
    fun testSemiFormattedTuples_WillFormatAsPartialJson_WhenCalledWithoutMatchingKeyValuePairs() {
        assertEquals(
                "my-message",
                semiFormattedTuples("my-message"))
    }

    @Test
    fun testSemiFormattedTuples_WillFormatAsPartialJson_WhenCalledWithMatchingKeyValuePairs() {
        assertEquals(
                "my-message\", \"key1\":\"value1\", \"key2\":\"value2",
                semiFormattedTuples("my-message", "key1", "value1", "key2", "value2"))
    }

    @Test
    fun testSemiFormattedTuples_WillEscapeJsonInMessageAndTupleValues_WhenCalled() {
        assertEquals(
                "This is almost unreadable, but a necessary test, sorry!",
                "message-\\/:'!@\\u00A3\$%^&*()\\n\\t\\r\", \"key-unchanged\":\"value-\\/:!@\\u00A3\$%^&*()\\n\\t\\r",
                semiFormattedTuples("message-/:'!@£\$%^&*()\n\t\r", "key-unchanged", "value-/:!@£\$%^&*()\n\t\r"))
    }

    @Test
    fun testSemiFormattedTuples_WillFailWithException_WhenCalledWithoutMatchingKeyValuePairs() {
        try {
            semiFormattedTuples("my-message", "key1")
            fail("Expected an IllegalArgumentException")
        } catch (expected: IllegalArgumentException) {
            assertEquals(
                    "Must have matched key-value pairs but had 1 argument(s)",
                    expected.message)
        }
    }

    @Test
    fun testLoggerUtils_Debug_WillFormatAsPartialJson_WhenCalled() {
        val mockLogger: org.slf4j.Logger = mock()

        logDebug(mockLogger, "main-message", "key1", "value1", "key2", "value2")

        verify(mockLogger, times(1)).debug("main-message\", \"key1\":\"value1\", \"key2\":\"value2")
        verifyNoMoreInteractions(mockLogger)
    }

    @Test
    fun testLoggerUtils_Info_WillFormatAsPartialJson_WhenCalled() {
        val mockLogger: org.slf4j.Logger = mock()

        logInfo(mockLogger, "main-message", "key1", "value1", "key2", "value2")

        verify(mockLogger, times(1)).info("main-message\", \"key1\":\"value1\", \"key2\":\"value2")
        verifyNoMoreInteractions(mockLogger)
    }

    @Test
    fun testLoggerUtils_Error_WillFormatAsPartialJson_WhenCalled() {
        val mockLogger: org.slf4j.Logger = mock()

        logError(mockLogger, "main-message", "key1", "value1", "key2", "value2")

        verify(mockLogger, times(1)).error("main-message\", \"key1\":\"value1\", \"key2\":\"value2")
        verifyNoMoreInteractions(mockLogger)
    }

    @Test
    fun testLoggerUtils_Error_WillFormatAsPartialJson_WhenCalledWithKeyValuePairsAndException() {
        val mockLogger: org.slf4j.Logger = mock()
        val exception = RuntimeException("boom")

        logError(mockLogger, "main-message", exception, "key1", "value1", "key2", "value2")

        verify(mockLogger, times(1)).error(eq("main-message\", \"key1\":\"value1\", \"key2\":\"value2"), same(exception))
        verifyNoMoreInteractions(mockLogger)
    }

    @Test
    fun testLoggerLayoutAppender_WillReturnEmpty_WhenCalledWithNothing() {
        val result = LoggerLayoutAppender().doLayout(null)
        assertEquals("", result)
    }

    @Test
    fun testLoggerLayoutAppender_WillReturnSkinnyJson_WhenCalledWithEmptyEvent() {
        val result = LoggerLayoutAppender().doLayout(mock<ILoggingEvent>())
        assertEquals(
                "{ timestamp:\"01:00:00.000\", thread:\"null\", log_level:\"null\", logger:\"null\", application:\"HTME\", message:\"null\" }\n",
                result)
    }

    @Test
    fun testLoggerLayoutAppender_WillFormatAsJson_WhenCalledWithVanillaMessage() {
        val mockEvent = mock<ILoggingEvent>()
        whenever(mockEvent.timeStamp).thenReturn(9876543210)
        whenever(mockEvent.level).thenReturn(Level.WARN)
        whenever(mockEvent.threadName).thenReturn("betty")
        whenever(mockEvent.loggerName).thenReturn("mavis")
        whenever(mockEvent.formattedMessage).thenReturn("your-message")

        val result = LoggerLayoutAppender().doLayout(mockEvent)
        assertEquals(
                "{ timestamp:\"08:29:03.210\", thread:\"betty\", log_level:\"WARN\", logger:\"mavis\", application:\"HTME\", message:\"your-message\" }\n",
                result)
    }

    @Test
    fun testLoggerLayoutAppender_WillFormatAsJson_WhenCalledWithEmbeddedTokenMessage() {
        val mockEvent = mock<ILoggingEvent>()
        whenever(mockEvent.timeStamp).thenReturn(9876543210)
        whenever(mockEvent.level).thenReturn(Level.WARN)
        whenever(mockEvent.threadName).thenReturn("betty")
        whenever(mockEvent.loggerName).thenReturn("mavis")
        val embeddedTokens = semiFormattedTuples("my-message", "key1", "value1", "key2", "value2")
        whenever(mockEvent.formattedMessage).thenReturn(embeddedTokens)

        val result = LoggerLayoutAppender().doLayout(mockEvent)
        assertEquals(
                "{ timestamp:\"08:29:03.210\", thread:\"betty\", log_level:\"WARN\", logger:\"mavis\", application:\"HTME\", message:\"my-message\", \"key1\":\"value1\", \"key2\":\"value2\" }\n",
                result)
    }

    @Test
    fun testLoggerLayoutAppender_ShouldNotEscapeTheJsonMessage_AsThatWouldMessWithOurCustomStaticLogMethodsWhichDo() {
        val mockEvent = mock<ILoggingEvent>()
        whenever(mockEvent.timeStamp).thenReturn(9876543210)
        whenever(mockEvent.level).thenReturn(Level.WARN)
        whenever(mockEvent.threadName).thenReturn("betty")
        whenever(mockEvent.loggerName).thenReturn("mavis")
        whenever(mockEvent.formattedMessage).thenReturn("message-/:'!@")

        val result = LoggerLayoutAppender().doLayout(mockEvent)
        assertEquals(
                "The standard logger should not escape json characters that Spring or AWS-utils might send it, sorry",
                "{ timestamp:\"08:29:03.210\", thread:\"betty\", log_level:\"WARN\", logger:\"mavis\", application:\"HTME\", message:\"message-/:'!@\" }\n",
                result)
    }
}
