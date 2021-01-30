package app.batch

import app.exceptions.ScanRetriesExhaustedException
import app.exceptions.BlockedTopicException
import app.utils.FilterBlockedTopicsUtils
import app.utils.TextUtils
import com.nhaarman.mockitokotlin2.*
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.apache.hadoop.hbase.NotServingRegionException
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Table
import org.junit.Test

import org.junit.Assert.*
import org.springframework.test.util.ReflectionTestUtils

class HBaseReaderTest {

    companion object {
        const val tableName = "database:collection"
        const val topicName = "db.database.collection"
        const val blockedTopicName = "db.crypto.encryptedData.unencrypted"
        const val blockedTopics = "db.crypto.encryptedData.unencrypted"
    }

    @Test
    fun onSuccessfulScanDoesNotRetry() {
        val result = mock<Result> {
            on { row } doReturn byteArrayOf(2)
        }

        val resultScanner = mock<ResultScanner> {
            on { next() } doReturn result
        }

        val table = mock<Table> {
            on { getScanner(any<Scan>()) } doReturn resultScanner
        }

        val filterBlockedTopicsUtils = FilterBlockedTopicsUtils()
        ReflectionTestUtils.setField(filterBlockedTopicsUtils, "blockedTopics", blockedTopics)

        val textUtils = TextUtils()
        val connection = mock<Connection> {
            on { getTable(TableName.valueOf(tableName)) } doReturn table
        }

        val hBaseReader = HBaseReader(connection, textUtils, filterBlockedTopicsUtils)
        ReflectionTestUtils.setField(hBaseReader, "scanRetrySleepMs", "1")
        ReflectionTestUtils.setField(hBaseReader, "scanMaxRetries", "5")
        ReflectionTestUtils.setField(hBaseReader, "topicName", topicName)
        ReflectionTestUtils.setField(hBaseReader, "start", 0)
        ReflectionTestUtils.setField(hBaseReader, "stop", 10)
        val spy = spy(hBaseReader)
        val actual = spy.read()
        verify(spy, times(1)).read()
        verify(connection, times(1)).getTable(TableName.valueOf(tableName))
        val argumentCaptor = argumentCaptor<Scan>()
        verify(table, times(1)).getScanner(argumentCaptor.capture())
        val scan = argumentCaptor.firstValue
        assertArrayEquals(byteArrayOf(0), scan.startRow)
        assertArrayEquals(byteArrayOf(10), scan.stopRow)
        assertArrayEquals(byteArrayOf(2), result.row)
        assertEquals(result, actual)
    }

    @Test
    fun onUnsuccessfulScanDoesRetry() {
        val firstResult = mock<Result> {
            on { row } doReturn byteArrayOf(2)
        }

        val failingScanner = mock<ResultScanner> {
            on { next() } doReturn firstResult doThrow NotServingRegionException("Error")
        }

        val secondResult = mock<Result> {
            on { row } doReturn byteArrayOf(5)
        }

        val successfulScanner = mock<ResultScanner> {
            on { next() } doReturn secondResult doReturn null
        }

        val table = mock<Table> {
            on { getScanner(any<Scan>()) } doReturn failingScanner doReturn successfulScanner
        }

        val connection = mock<Connection> {
            on { getTable(TableName.valueOf(tableName)) } doReturn table
        }

        val textUtils = TextUtils()

        val filterBlockedTopicsUtils = FilterBlockedTopicsUtils()
        ReflectionTestUtils.setField(filterBlockedTopicsUtils, "blockedTopics", blockedTopics)

        val hBaseReader = HBaseReader(connection, textUtils, filterBlockedTopicsUtils)
        ReflectionTestUtils.setField(hBaseReader, "scanRetrySleepMs", "1")
        ReflectionTestUtils.setField(hBaseReader, "scanMaxRetries", "5")
        ReflectionTestUtils.setField(hBaseReader, "topicName", topicName)
        ReflectionTestUtils.setField(hBaseReader, "start", 0)
        ReflectionTestUtils.setField(hBaseReader, "stop", 10)

        val spy = spy(hBaseReader)
        val actualFirstResult = spy.read()
        val actualSecondResult = spy.read()
        val actualThirdResult = spy.read()
        val argumentCaptor = argumentCaptor<Scan>()
        verify(table, times(2)).getScanner(argumentCaptor.capture())
        val firstScan = argumentCaptor.firstValue
        val secondScan = argumentCaptor.secondValue

        assertArrayEquals(byteArrayOf(0), firstScan.startRow)
        assertArrayEquals(byteArrayOf(2), secondScan.startRow)
        assertNotNull(actualFirstResult)
        assertNotNull(actualSecondResult)
        assertNull(actualThirdResult)
        assertArrayEquals(byteArrayOf(2), actualFirstResult!!.row)
        assertArrayEquals(byteArrayOf(5), actualSecondResult!!.row)
    }

    @Test(expected = ScanRetriesExhaustedException::class)
    fun onUnsuccessfulScanGivesUpAfterMaxRetries() {

        val firstResult = mock<Result> {
            on { row } doReturn byteArrayOf(3)
        }

        val firstFailingScanner = mock<ResultScanner> {
            on { next() } doReturn firstResult doThrow NotServingRegionException("Error")
        }


        val secondFailingScanner = mock<ResultScanner> {
            on { next() } doThrow NotServingRegionException("Error")
        }

        val table = mock<Table> {
            on { getScanner(any<Scan>()) } doReturn firstFailingScanner doReturn secondFailingScanner
        }

        val connection = mock<Connection> {
            on { getTable(TableName.valueOf(tableName)) } doReturn table
        }

        val textUtils = TextUtils()

        val filterBlockedTopicsUtils = FilterBlockedTopicsUtils()

        val hBaseReader = HBaseReader(connection, textUtils, filterBlockedTopicsUtils)
        ReflectionTestUtils.setField(hBaseReader, "scanRetrySleepMs", "1")
        ReflectionTestUtils.setField(hBaseReader, "scanMaxRetries", "5")
        ReflectionTestUtils.setField(hBaseReader, "topicName", topicName)
        ReflectionTestUtils.setField(hBaseReader, "start", 0)
        ReflectionTestUtils.setField(hBaseReader, "stop", 10)
        ReflectionTestUtils.setField(filterBlockedTopicsUtils, "blockedTopics", blockedTopics)

        try {
            val spy = spy(hBaseReader)
            while (true) {
                spy.read()
            }
        }
        finally {
            val argumentCaptor = argumentCaptor<Scan>()
            verify(table, times(5)).getScanner(argumentCaptor.capture())
            val firstArg = argumentCaptor.firstValue
            assertArrayEquals(byteArrayOf(0), firstArg.startRow)
            val subsequentArgs = argumentCaptor.allValues.drop(1)
            assertEquals(4, subsequentArgs.size)
            subsequentArgs.forEach {
                assertArrayEquals(byteArrayOf(3), it.startRow)
            }
        }
    }

    @Test
    fun throwsFilterBlockedTopicException() {

        val table = mock<Table>()

        val connection = mock<Connection> {
            on { getTable(TableName.valueOf(tableName)) } doReturn table
        }

        val filterBlockedTopicsUtils = FilterBlockedTopicsUtils()
        ReflectionTestUtils.setField(filterBlockedTopicsUtils, "blockedTopics", blockedTopics)

        val textUtils = TextUtils()

        val hBaseReader = HBaseReader(connection, textUtils, filterBlockedTopicsUtils)
        ReflectionTestUtils.setField(hBaseReader, "scanRetrySleepMs", "1")
        ReflectionTestUtils.setField(hBaseReader, "scanMaxRetries", "5")
        ReflectionTestUtils.setField(hBaseReader, "topicName", blockedTopicName)
        ReflectionTestUtils.setField(hBaseReader, "start", 0)
        ReflectionTestUtils.setField(hBaseReader, "stop", 10)

        val exception = shouldThrow<BlockedTopicException> {
            val spy = spy(hBaseReader)
            while (true) {
                spy.read()
            }
        }
        exception.message shouldBe "Provided topic is blocked so cannot be processed: 'db.crypto.encryptedData.unencrypted'"
    }
}
