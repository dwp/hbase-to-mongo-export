package app.batch

import app.exceptions.ScanRetriesExhaustedException
import app.utils.TextUtils
import com.nhaarman.mockitokotlin2.*
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
    }

    @Test
    fun onSuccessfulScanDoesNotRetry() {
        val result = mock<Result> {
            on { row } doReturn byteArrayOf(2)
        }

        val resultScanner = mock<ResultScanner>() {
            on { next() } doReturn result
        }

        val table = mock<Table> {
            on { getScanner(any<Scan>()) } doReturn resultScanner
        }

        val textUtils = TextUtils()
        val connection = mock<Connection> {
            on { getTable(TableName.valueOf(tableName)) } doReturn table
        }

        val hBaseReader = HBaseReader(connection, textUtils)
        ReflectionTestUtils.setField(hBaseReader, "scanRetrySleepMs", "1")
        ReflectionTestUtils.setField(hBaseReader, "scanMaxRetries", "5")
        ReflectionTestUtils.setField(hBaseReader, "topicName", topicName)
        ReflectionTestUtils.setField(hBaseReader, "start", 0)
        ReflectionTestUtils.setField(hBaseReader, "stop", 10)
        val spy = spy(hBaseReader)
        var actual = spy.read()
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

        val failingScanner = mock<ResultScanner>() {
            on { next() } doReturn firstResult doThrow NotServingRegionException("Error")
        }

        val secondResult = mock<Result> {
            on { row } doReturn byteArrayOf(5)
        }

        val successfulScanner = mock<ResultScanner>() {
            on { next() } doReturn secondResult doReturn null
        }

        val table = mock<Table> {
            on { getScanner(any<Scan>()) } doReturn failingScanner doReturn successfulScanner
        }

        val connection = mock<Connection> {
            on { getTable(TableName.valueOf(tableName)) } doReturn table
        }

        val hBaseReader = HBaseReader(connection, TextUtils())
        ReflectionTestUtils.setField(hBaseReader, "scanRetrySleepMs", "1")
        ReflectionTestUtils.setField(hBaseReader, "scanMaxRetries", "5")
        ReflectionTestUtils.setField(hBaseReader, "topicName", topicName)
        ReflectionTestUtils.setField(hBaseReader, "start", 0)
        ReflectionTestUtils.setField(hBaseReader, "stop", 10)

        val spy = spy(hBaseReader)
        var actualFirstResult = spy.read()
        var actualSecondResult = spy.read()
        var actualThirdResult = spy.read()
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

        val hBaseReader = HBaseReader(connection, TextUtils())
        ReflectionTestUtils.setField(hBaseReader, "scanRetrySleepMs", "1")
        ReflectionTestUtils.setField(hBaseReader, "scanMaxRetries", "5")
        ReflectionTestUtils.setField(hBaseReader, "topicName", topicName)
        ReflectionTestUtils.setField(hBaseReader, "start", 0)
        ReflectionTestUtils.setField(hBaseReader, "stop", 10)
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
}
