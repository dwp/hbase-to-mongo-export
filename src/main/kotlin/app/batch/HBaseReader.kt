package app.batch

import app.exceptions.ScanRetriesExhaustedException
import app.exceptions.TopicIsBlockedException
import app.utils.FilterBlockedTopicsUtils
import app.utils.TextUtils
import app.utils.logging.logError
import app.utils.logging.logInfo
import app.utils.logging.logWarn
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.annotation.AfterStep
import org.springframework.batch.core.annotation.BeforeStep
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.ZonedDateTime

@Component
@StepScope
class HBaseReader(private val connection: Connection, private val textUtils: TextUtils, private val filterBlockedTopicsUtils: FilterBlockedTopicsUtils) : ItemReader<Result> {

    override fun read(): Result? =
        try {
            val result = scanner().next()
            if (result != null) {
                latestId = result.row
            }
            retryAttempts = 0
            result
        }
        catch (e: TopicIsBlockedException) {
            logWarn(logger, "Provided topic is blocked so cannot be processed",
                    "exception", e.message ?: "",
                    "topic_name", topicName
            )
            throw e
        }
        catch (e: Exception) {
            reopenScannerAndRetry(e)
        }

    private fun reopenScannerAndRetry(e: Exception): Result? {
        val lastKey = latestId ?: byteArrayOf(start.toByte())
        return if (++retryAttempts < scanMaxRetries.toInt()) {
            logWarn(logger, "Failed to get next record, reopening scanner",
                    "exception", e.message ?: "",
                    "attempt", "$retryAttempts",
                    "max_attempts", scanMaxRetries,
                    "latest_id", printableKey(lastKey))

            scanner?.close()
            Thread.sleep(scanRetrySleepMs.toLong())
            scanner = newScanner(lastKey)
            read()
        }
        else {
            logError(logger, "Failed to get next record after max retries", e,
                    "exception", e.message ?: "",
                    "attempt", "$retryAttempts",
                    "max_attempts", scanMaxRetries,
                    "latest_id", printableKey(lastKey))
            throw ScanRetriesExhaustedException(printableKey(lastKey), retryAttempts, e)
        }
    }


    private var latestId: ByteArray? = null
    private var retryAttempts = 0

    @BeforeStep
    fun beforeStep(stepExecution: StepExecution) {
        start = stepExecution.executionContext["start"] as Int
        stop = stepExecution.executionContext["stop"] as Int
    }

    @AfterStep
    fun afterStep() {
        logInfo(logger, "Closing scanner", "start", "$start", "stop", "$stop")
        scanner().close()
    }

    @Synchronized
    fun scanner(): ResultScanner {
        if (scanner == null) {
            scanner = newScanner(byteArrayOf(start.toByte()))
        }
        return scanner!!
    }

    private fun newScanner(start: ByteArray): ResultScanner {
        filterBlockedTopicsUtils.isTopicBlocked(topicName)
        val matcher = textUtils.topicNameTableMatcher(topicName)
        val namespace = matcher?.groupValues?.get(1)
        val tableName = matcher?.groupValues?.get(2)
        val qualifiedTableName = "$namespace:$tableName".replace("-", "_")
        val table = connection.getTable(TableName.valueOf(qualifiedTableName))
        return table.getScanner(scan(start))
    }

    fun getScanTimeRangeStartEpoch() =
            if (scanTimeRangeStart.isNotBlank())
                ZonedDateTime.parse(scanTimeRangeStart).toInstant().toEpochMilli()
            else 0

    fun getScanTimeRangeEndEpoch(): Long {
        var endDateTime = ZonedDateTime.now()
        if (scanTimeRangeEnd.isNotBlank()) {
            endDateTime = ZonedDateTime.parse(scanTimeRangeEnd)
        }

        return endDateTime.toInstant().toEpochMilli()
    }

    private fun scan(startId: ByteArray): Scan {
        val timeStart = getScanTimeRangeStartEpoch()
        val timeEnd = getScanTimeRangeEndEpoch()

        val scan = Scan().apply {
            setTimeRange(timeStart, timeEnd)

            if (useTimelineConsistency.toBoolean()) {
                consistency = Consistency.TIMELINE
            }

            withStartRow(startId, false)

            if (stop != 0) {
                withStopRow(byteArrayOf(stop.toByte()), false)
            }

            cacheBlocks = scanCacheBlocks.toBoolean()
            if (scanCacheSize.toInt() > 0) {
                caching = scanCacheSize.toInt()
            }

            if (scanMaxResultSize.toInt() > 0) {
                maxResultSize = scanMaxResultSize.toLong()
            }

            allowPartialResults = partialResultsAllowed.toBoolean()

        }

        logInfo(logger, "Scan caching config",
                "scan_caching", "${scan.caching}",
                "scan.maxResultSize", "${scan.maxResultSize}",
                "cache_blocks", "${scan.cacheBlocks}",
                "start", "$start",
                "stop", "$stop",
                "scan_time_range_start", timeStart.toString(),
                "scan_time_range_end", timeEnd.toString(),
                "use_timeline_consistency", useTimelineConsistency)

        return scan
    }

    fun printableKey(key: ByteArray) =
            if (key.size > 4) {
                val hash = key.slice(IntRange(0, 3))
                val hex = hash.map { String.format("\\x%02X", it) }.joinToString("")
                val renderable = key.slice(IntRange(4, key.size - 1)).map { it.toChar() }.joinToString("")
                "${hex}${renderable}"
            }
            else {
                String(key)
            }

    private var scanner: ResultScanner? = null

    @Value("\${scan.time.range.start:}")
    private var scanTimeRangeStart: String = ""

    @Value("\${scan.time.range.end:}")
    private var scanTimeRangeEnd: String = ""

    @Value("\${topic.name}")
    private var topicName: String = ""

    @Value("\${scan.cache.size:-1}")
    private var scanCacheSize: String = "-1"

    @Value("\${scan.max.result.size:-1}")
    private var scanMaxResultSize: String = "-1"

    @Value("\${scan.max.retries:100}")
    private var scanMaxRetries: String = "100"

    @Value("\${scan.retry.sleep.ms:10000}")
    private var scanRetrySleepMs: String = "10000"

    @Value("\${scan.cache.blocks:true}")
    private var scanCacheBlocks: String = "true"

    @Value("\${use.timeline.consistency:true}")
    private var useTimelineConsistency: String = "true"

    @Value("\${allow.partial.results:true}")
    private var partialResultsAllowed: String = "true"

    private var start: Int = Int.MIN_VALUE
    private var stop: Int = Int.MAX_VALUE

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseReader::class.toString())
    }
}
