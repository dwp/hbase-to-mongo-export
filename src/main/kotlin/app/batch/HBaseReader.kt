package app.batch

import app.exceptions.BlockedTopicException
import app.exceptions.ScanRetriesExhaustedException
import app.utils.FilterBlockedTopicsUtils
import app.utils.PropertyUtility
import app.utils.TextUtils
import io.prometheus.client.Counter
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.TableNotEnabledException
import org.apache.hadoop.hbase.TableNotFoundException
import org.apache.hadoop.hbase.client.*
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.annotation.AfterStep
import org.springframework.batch.core.annotation.BeforeStep
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.time.ZonedDateTime
import java.util.*
import kotlin.math.absoluteValue
import org.apache.hadoop.hbase.client.metrics.ScanMetrics
import org.apache.hadoop.hbase.ClusterMetrics
import org.apache.hadoop.hbase.client.Admin

@Component
@StepScope
class HBaseReader(private val connection: Connection,
                  private val textUtils: TextUtils,
                  private val filterBlockedTopicsUtils: FilterBlockedTopicsUtils,
                  private val scanRetriesCounter: Counter,
                  private val failedScansCounter: Counter): ItemReader<Result> {

    @Throws(TableNotFoundException::class, TableNotEnabledException::class, BlockedTopicException::class)
    override fun read(): Result? =
        try {
            collectClusterMetrics(connection)
            val scanner = scanner().next()
            val scanMetrics = scanner.getScanMetrics()
            logger.info("scan metrics", countOfRPCcalls to "${scanMetrics.countOfRPCcalls}",
            "countOfRemoteRPCcalls" to "${scanMetrics.countOfRemoteRPCcalls}",
            "sumOfMillisSecBetweenNexts" to "${scanMetrics.sumOfMillisSecBetweenNexts}",
            "countOfBytesInResults" to "${scanMetrics.countOfBytesInResults}",
            "countOfBytesInRemoteResults" to "${countOfBytesInRemoteResults}",
            "countOfRegions" to "${scanMetrics.countOfRegions}",
            "countOfRPCRetries" to "${scanMetrics.countOfRPCRetries}",
            "countOfRemoteRPCRetries" to "${scanMetrics.countOfRemoteRPCRetries}")

            val result = scanner().next()
            if (result != null) {
                latestId = result.row
            }
            retryAttempts = 0
            result
        } catch (e: BlockedTopicException) {
            logger.error("Provided topic is blocked so cannot be processed",
                "exception" to (e.message ?: ""),
                "topic_name" to topicName)
            throw e
        } catch (e: TableNotFoundException) {
            logger.error("Table does not exist for the provided topic",
                "exception" to (e.message ?: ""),
                "topic_name" to topicName)
            throw e
        } catch (e: TableNotEnabledException) {
            logger.error("Table is not enabled for the provided topic",
                "exception" to (e.message ?: ""),
                "topic_name" to topicName)
            throw e
        } catch (e: Exception) {
            logger.error("Error with scanner", e)
            reopenScannerAndRetry(e)
        }
    private fun collectClusterMetrics(connection: Connection){
        val admin: Admin = connection.getAdmin()
        val metrics: ClusterMetrics = admin.getClusterMetrics()
        logger.info("cluster metrics", "cluster id" to "${metrics.getClusterId()}",
        "live server metrics" to "${metrics.getLiveServerMetrics()}",
        "Region count" to "${metrics.getRegionCount()}",
        "Request count" to "${metrics.getRequestCount()}",
        "Servers name" to "${metrics.getServersName()}",
        "Region states count" to "${metrics.getTableRegionStatesCount()}")
    }

    private fun reopenScannerAndRetry(e: Exception): Result? {
        try {
            val lastKey = latestId ?: byteArrayOf(start.toByte())
            return if (++retryAttempts < scanMaxRetries.toInt()) {
                logger.warn("Failed to get next record, reopening scanner",
                    "exception" to (e.message ?: ""),
                    "attempt" to "$retryAttempts",
                    "max_attempts" to scanMaxRetries,
                    "latest_id" to printableKey(lastKey))

                scanner?.close()
                Thread.sleep(scanRetrySleepMs.toLong())
                scanner = newScanner(lastKey)
                scanRetriesCounter.labels(split()).inc()
                read()
            } else {
                logger.error("Failed to get next record after max retries", e,
                    "exception" to (e.message ?: ""),
                    "attempt" to "$retryAttempts",
                    "max_attempts" to scanMaxRetries,
                    "latest_id" to printableKey(lastKey))
                failedScansCounter.labels(split()).inc()
                throw ScanRetriesExhaustedException(printableKey(lastKey), retryAttempts, e)
            }
        } catch (e: Exception) {
            logger.error("Failed to open scanner", e)
            throw e
        }
    }

    private var latestId: ByteArray? = null
    private var retryAttempts = 0
    private var scanTimeRangeEndDefault = "2099-01-01T00:00:00.000Z"
    private var absoluteStart: Int = Int.MIN_VALUE
    private var absoluteStop: Int = Int.MAX_VALUE

    @BeforeStep
    fun beforeStep(stepExecution: StepExecution) {
        start = stepExecution.executionContext["start"] as Int
        stop = stepExecution.executionContext["stop"] as Int
        absoluteStart = (stepExecution.executionContext["start"] as Int).absoluteValue
        absoluteStop = (stepExecution.executionContext["stop"] as Int).absoluteValue
    }

    @AfterStep
    fun afterStep() {
        logger.info("Closing scanner", "start" to "$start", "stop" to "$stop")
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
        var qualifiedTableName = "$namespace:$tableName".replace("-", "_")
        qualifiedTableName = shardCalculationPartsCollection(topicName, qualifiedTableName)
        val table = connection.getTable(TableName.valueOf(qualifiedTableName))
        return table.getScanner(scan(start))
    }

    fun shardCalculationPartsCollection(topicName: String, qualifiedTableName: String): String {
        var table_name = qualifiedTableName
        logger.info("Started sharding calculationParts collection")
        if(topicName.contains("db.calculator.calculationParts", ignoreCase = true)) {
            table_name = "calculator:calculationParts"
            logger.info("set table to calculator:calculationParts")
            if (topicName.contains("db.calculator.calculationParts-q1-2021")) {
                scanTimeRangeEnd = "2021-04-01T00:00:00.000Z"
                logger.info("Picked up collection db.calculator.calculationParts-q1-2021")

            }
            if (topicName.contains("db.calculator.calculationParts-Apr-2021")) {
                scanTimeRangeStart = "2021-04-01T00:00:00.000Z"
                scanTimeRangeEnd = "2021-05-01T00:00:00.000Z"
                logger.info("Picked up collection db.calculator.calculationParts-Apr-2021")

            }
            if (topicName.contains("db.calculator.calculationParts-May-2021")) {
                scanTimeRangeStart = "2021-05-01T00:00:00.000Z"
                scanTimeRangeEnd = "2021-06-01T00:00:00.000Z"
                logger.info("Picked up collection db.calculator.calculationParts-May-2021")

            }
            if (topicName.contains("db.calculator.calculationParts-Jun-2021")) {
                scanTimeRangeStart = "2021-06-01T00:00:00.000Z"
                scanTimeRangeEnd = "2021-07-01T00:00:00.000Z"
                logger.info("Picked up collection db.calculator.calculationParts-Jun-2021")

            }
            if (topicName.contains("db.calculator.calculationParts-Jul-2021")) {
                scanTimeRangeStart = "2021-07-01T00:00:00.000Z"
                scanTimeRangeEnd = "2021-08-01T00:00:00.000Z"
                logger.info("Picked up collection db.calculator.calculationParts-Jul-2021")

            }
            if (topicName.contains("db.calculator.calculationParts-Aug-2021")) {
                scanTimeRangeStart = "2021-08-01T00:00:00.000Z"
                scanTimeRangeEnd = "2021-09-01T00:00:00.000Z"
                logger.info("Picked up collection db.calculator.calculationParts-Aug-2021")

            }
            if (topicName.contains("db.calculator.calculationParts-Sep-2021")) {
                scanTimeRangeStart = "2021-09-01T00:00:00.000Z"
                scanTimeRangeEnd = "2021-10-01T00:00:00.000Z"
                logger.info("Picked up collection db.calculator.calculationParts-Sep-2021")

            }
            if (topicName.contains("db.calculator.calculationParts-Oct-2021")) {
                scanTimeRangeStart = "2021-10-01T00:00:00.000Z"
                scanTimeRangeEnd = "2021-11-01T00:00:00.000Z"
                logger.info("Picked up collection db.calculator.calculationParts-Oct-2021")

            }
            if (topicName.contains("db.calculator.calculationParts-Nov-2021")) {
                scanTimeRangeStart = "2021-11-01T00:00:00.000Z"
                scanTimeRangeEnd = "2021-12-01T00:00:00.000Z"
                logger.info("Picked up collection db.calculator.calculationParts-Nov-2021")

            }
            if (topicName.contains("db.calculator.calculationParts-Dec-2021")) {
                scanTimeRangeStart = "2021-12-01T00:00:00.000Z"
                scanTimeRangeEnd = "2022-01-01T00:00:00.000Z"
                logger.info("Picked up collection db.calculator.calculationParts-Dec-2021")

            }
            if (topicName.contains("db.calculator.calculationParts-q1-2022")) {
                scanTimeRangeStart = "2022-01-01T00:00:00.000Z"
                logger.info("Picked up collection db.calculator.calculationParts-q1-2022")
            }
        }
        return table_name
    }
    fun getScanTimeRangeStartEpoch() =
        if (scanTimeRangeStart.isNotBlank())
            ZonedDateTime.parse(scanTimeRangeStart).toInstant().toEpochMilli()
        else 0

    fun getScanTimeRangeEndEpoch(): Long {
        var endDateTime = ZonedDateTime.parse(scanTimeRangeEndDefault)
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
            setScanMetricsEnabled(true)

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
            id = "${PropertyUtility.correlationId()}/${Date().time}/$topicName/$absoluteStart/$absoluteStop"
        }

        logger.info("Scan caching config",
            "scan_id" to (scan.id ?: ""),
            "scan_caching" to "${scan.caching}",
            "scan.maxResultSize" to "${scan.maxResultSize}",
            "cache_blocks" to "${scan.cacheBlocks}",
            "start" to "$start",
            "stop" to "$stop",
            "scan_time_range_start" to timeStart.toString(),
            "scan_time_range_end" to timeEnd.toString(),
            "use_timeline_consistency" to useTimelineConsistency)

        return scan
    }

    fun printableKey(key: ByteArray) =
        if (key.size > 4) {
            val hash = key.slice(IntRange(0, 3))
            val hex = hash.joinToString("") { String.format("\\x%02X", it) }
            val renderable = key.slice(IntRange(4, key.size - 1)).map(Byte::toChar).joinToString("")
            "${hex}${renderable}"
        } else {
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

    private fun split() = "%03d-%03d".format(absoluteStart, absoluteStop)

    companion object {
        val logger = DataworksLogger.getLogger(HBaseReader::class)
    }
}
