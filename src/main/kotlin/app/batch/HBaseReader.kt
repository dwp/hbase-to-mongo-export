package app.batch

import app.utils.TextUtils
import app.utils.logging.logInfo
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
class HBaseReader(private val connection: Connection, private val textUtils: TextUtils) : ItemReader<Result> {

    private var start: Int = Int.MIN_VALUE
    private var stop: Int = Int.MAX_VALUE

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

    override fun read(): Result? = scanner().next()

    fun resetScanner() {
        scanner = null
    }

    @Synchronized
    fun scanner(): ResultScanner {
        if (scanner == null) {
            scanner = newScanner(start)
        }
        return scanner!!
    }

    private fun newScanner(start: Int): ResultScanner {
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

    private fun scan(startId: Int): Scan {
        val timeStart = getScanTimeRangeStartEpoch()
        val timeEnd = getScanTimeRangeEndEpoch()

        val scan = Scan().apply {
            setTimeRange(timeStart, timeEnd)

            if (useTimelineConsistency.toBoolean()) {
                consistency = Consistency.TIMELINE
            }

            withStartRow(byteArrayOf(startId.toByte()), true)
            if (stop != 0) {
                withStopRow(byteArrayOf(stop.toByte()), true)
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

    @Value("\${scan.cache.blocks:true}")
    private var scanCacheBlocks: String = "true"

    @Value("\${use.timeline.consistency:true}")
    private var useTimelineConsistency: String = "true"

    @Value("\${allow.partial.results:true}")
    private var partialResultsAllowed: String = "true"

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseReader::class.toString())
    }
}
