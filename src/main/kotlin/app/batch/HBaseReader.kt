package app.batch

import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.MissingFieldException
import app.utils.TextUtils
import app.utils.logging.logError
import app.utils.logging.logInfo
import com.google.gson.Gson
import com.google.gson.JsonObject
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.annotation.BeforeStep
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.nio.charset.Charset
import java.util.*

@Component
@StepScope
class HBaseReader constructor(private val connection: Connection): ItemReader<SourceRecord> {

    private var start: Int = Int.MIN_VALUE
    private var stop: Int = Int.MAX_VALUE

    @BeforeStep
    fun beforeStep(stepExecution: StepExecution) {
        start = stepExecution.executionContext["start"] as Int
        stop = stepExecution.executionContext["stop"] as Int
    }

    var recordCount = 0

    override fun read() =
        scanner().next()?.let { result ->
            recordCount++

            if (recordCount % 10000 == 0) {
                logInfo(logger, "$this: Processed records for topic", "record_count", "$recordCount", "topic_name", topicName)
            }

            val idBytes = result.row
            result.advance()
            val cell = result.current()
            val timestamp = cell.timestamp
            cell.qualifierArray
            val value = result.value()
            val json = value.toString(Charset.defaultCharset())
            val dataBlock = Gson().fromJson(json, JsonObject::class.java)
            val messageInfo = dataBlock.getAsJsonObject("message")
            val encryptedDbObject = messageInfo.getAsJsonPrimitive("dbObject")?.asString
            val db = messageInfo.getAsJsonPrimitive("db")?.asString
            val collection = messageInfo.getAsJsonPrimitive("collection")?.asString
            val lastModified = lastModifiedDateTime(messageInfo)
            val encryptionInfo = messageInfo.getAsJsonObject("encryption")
            val encryptedEncryptionKey = encryptionInfo.getAsJsonPrimitive("encryptedEncryptionKey").asString
            val keyEncryptionKeyId = encryptionInfo.getAsJsonPrimitive("keyEncryptionKeyId").asString
            val initializationVector = encryptionInfo.getAsJsonPrimitive("initialisationVector").asString

            if (encryptedDbObject.isNullOrEmpty()) {
                logError(logger, "Missing dbObject field, skipping this record", "id_bytes", "$idBytes")
                throw MissingFieldException(idBytes, "dbObject")
            }
            if (db.isNullOrEmpty()) {
                logError(logger, "Missing db field, skipping this record", "id_bytes", "$idBytes")
                throw MissingFieldException(idBytes, "db")
            }
            if (collection.isNullOrEmpty()) {
                logError(logger, "Missing collection field, skipping this record", "id_bytes", "$idBytes")
                throw MissingFieldException(idBytes, "collection")
            }

            val encryptionBlock = EncryptionBlock(keyEncryptionKeyId, initializationVector, encryptedEncryptionKey)
            SourceRecord(idBytes, timestamp, encryptionBlock, encryptedDbObject, db, collection, lastModified)
        }


    fun lastModifiedDateTime(messageObject: JsonObject): String {
        val lastModifiedElement = messageObject.get("_lastModifiedDateTime")
        val epoch = "1980-01-01T00:00:00.000Z"
        return if (lastModifiedElement != null) {
            if (lastModifiedElement.isJsonPrimitive) {
                lastModifiedElement.asJsonPrimitive.asString
            }
            else {
                val asObject = lastModifiedElement.asJsonObject
                val dateSubField = "\$date"
                asObject.getAsJsonPrimitive(dateSubField)?.asString ?: epoch
            }
        }
        else {
            epoch
        }
    }

    fun resetScanner() {
        scanner = null
    }

    @Synchronized
    fun scanner(): ResultScanner {
        if (scanner == null) {
            logInfo(logger, "Getting data table from hbase connection", "connection", "$connection", "topic_name", topicName)
            val matcher = textUtils.topicNameTableMatcher(topicName)
            if (matcher != null) {
                val namespace = matcher.groupValues[1]
                val tableName = matcher.groupValues[2]
                val qualifiedTableName = "$namespace:$tableName".replace("-", "_")
                val table = connection.getTable(TableName.valueOf(qualifiedTableName))
                scanner = table.getScanner(scan())
            }
        }
        return scanner!!
    }


    private fun scan(): Scan {
        val scan = Scan().apply {
            setTimeRange(0, Date().time)

            if (start == -1 && stop == -1) {
                setRowPrefixFilter(byteArrayOf(-1))
            }
            else {
                withStartRow(byteArrayOf(start.toByte()), true)
                if (stop != 0) {
                    withStopRow(byteArrayOf(stop.toByte()), true)
                }
            }

            cacheBlocks = scanCacheBlocks.toBoolean()
            if (scanCacheSize.toInt() > 0) {
                caching = scanCacheSize.toInt()
            }

            if (scanMaxResultSize.toInt() > 0) {
                maxResultSize = scanMaxResultSize.toLong()
            }
        }

        logInfo(logger, "Scan caching config",
            "scan_caching", "${scan.caching}",
            "scan.maxResultSize", "${scan.maxResultSize}",
            "cache_blocks", "${scan.cacheBlocks}",
            "useLatest", useLatest,
            "start", "$start",
            "stop", "$stop")

        return scan
    }

    private var scanner: ResultScanner? = null

    @Value("\${topic.name}")
    private lateinit var topicName: String // i.e. "db.user.data"

    @Value("\${scan.cache.size:-1}")
    private lateinit var scanCacheSize: String

    @Value("\${scan.max.result.size:-1}")
    private lateinit var scanMaxResultSize: String

    @Value("\${scan.cache.blocks:true}")
    private lateinit var scanCacheBlocks: String

    @Value("\${latest.available:false}")
    private lateinit var useLatest: String

    private val textUtils = TextUtils()

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseReader::class.toString())
    }
}
