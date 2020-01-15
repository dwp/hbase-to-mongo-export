package app.batch

import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.MissingFieldException
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
import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.nio.charset.Charset
import java.util.*

@Component
class HBaseReader constructor(private val connection: Connection) : ItemReader<SourceRecord> {

    var recordCount = 0
    val start = System.currentTimeMillis()
    var start_time_milliseconds = System.setProperty("start_time_milliseconds", "$start")

    override fun read() =
        scanner().next()?.let { result ->
            recordCount++

            if (recordCount % 10000 == 0) {
                logInfo(logger, "Processed records for topic", "record_count", "$recordCount", "topic_name", topicName)
            }

            val idBytes = result.row
            result.advance() //move pointer to the first cell
            val timestamp = result.current().timestamp
            val value = result.getValue(columnFamily.toByteArray(), topicName.toByteArray())
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
            } else {
                val asObject = lastModifiedElement.asJsonObject
                val dateSubField = "\$date"
                asObject.getAsJsonPrimitive(dateSubField)?.asString ?: epoch
            }
        } else {
            epoch
        }
    }

    fun resetScanner() {
        scanner = null
    }

    @Synchronized
    fun scanner(): ResultScanner {
        if (scanner == null) {
            logInfo(logger, "Getting data table from hbase connection", "connection", "$connection", "data_table_name", dataTableName, "column_family", columnFamily, "topic_name", topicName)
            val table = connection.getTable(TableName.valueOf(dataTableName))
            val scan = Scan().apply {
                addColumn(columnFamily.toByteArray(), topicName.toByteArray())
                if (!useLatest.toBoolean()) {
                    setTimeRange(0, Date().time)
                }
            }

            if (scanCacheSize.toInt() > 0) {
                scan.caching = scanCacheSize.toInt()
            }

            if (scanMaxResultSize.toInt() > 0) {
                scan.maxResultSize = scanMaxResultSize.toLong()
            }

            scan.cacheBlocks = scanCacheBlocks.toBoolean()
            scan.isAsyncPrefetch = asyncPrefetch.toBoolean()

            logInfo(logger, "Scan caching config",
                    "scan_caching", "${scan.caching}",
                    "scan.maxResultSize", "${scan.maxResultSize}",
                    "cache_blocks", "${scan.cacheBlocks}",
                    "async_prefetch", scan.isAsyncPrefetch.toString(),
                    "useLatest", useLatest)

            scanner = table.getScanner(scan)
        }
        return scanner!!
    }

    private var scanner: ResultScanner? = null

    @Value("\${column.family}")
    private lateinit var columnFamily: String // i.e. "topic"

    @Value("\${topic.name}")
    private lateinit var topicName: String // i.e. "db.user.data"

    @Value("\${scan.cache.size:-1}")
    private lateinit var scanCacheSize: String

    @Value("\${scan.max.result.size:-1}")
    private lateinit var scanMaxResultSize: String

    @Value("\${scan.cache.blocks:true}")
    private lateinit var scanCacheBlocks: String

    @Value("\${scan.async.prefetch:true}")
    private lateinit var asyncPrefetch: String

    @Value("\${latest.available:true}")
    private lateinit var useLatest: String

    @Value("\${data.table.name}")
    private lateinit var dataTableName: String


    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseReader::class.toString())
    }
}
