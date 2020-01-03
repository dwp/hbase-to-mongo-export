package app.batch

import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.MissingFieldException
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
import com.google.common.collect.Iterables;
import com.google.gson.JsonPrimitive
import org.apache.hadoop.hbase.HConstants

@Component
class HBaseReader constructor(private val connection: Connection) : ItemReader<SourceRecord> {

    var count = 0
    override fun read() =
        scanner().next()?.let { result ->
            count++

            if(count % 10000 == 0) {
                logger.info("Processed $count records for topic $topicName")
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
                logger.error("'$idBytes' missing dbObject field, skipping this record.")
                throw MissingFieldException(idBytes, "dbObject")
            }
            if (db.isNullOrEmpty()) {
                logger.error("'$idBytes' missing db field, skipping this record.")
                throw MissingFieldException(idBytes, "db")
            }
            if (collection.isNullOrEmpty()) {
                logger.error("'$idBytes' missing collection field, skipping this record.")
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
            logger.info("Getting '$dataTableName' table from '$connection'.")
            logger.info("columnFamily: '$columnFamily', topicName: '$topicName'.")
            val table = connection.getTable(TableName.valueOf(dataTableName))
            val scan = Scan().apply {
                addColumn(columnFamily.toByteArray(), topicName.toByteArray())
            }

//            if (scanCacheSize.toInt() > 0) {
//                scan.caching = scanCacheSize.toInt()
//            }
//
//            scan.maxResultSize = Long.MAX_VALUE
//            scan.cacheBlocks = false
//            logger.info("Scan cache size: '${scan.caching}'.")
//
//            logger.info("cache blocks: '${scan.cacheBlocks}'.")
            scanner = table.getScanner(scan)
        }
        return scanner!!
    }

    private var scanner: ResultScanner? = null

    @Value("\${column.family}")
    private lateinit var columnFamily: String // i.e. "topic"

    @Value("\${topic.name}")
    private lateinit var topicName: String // i.e. "db.user.data"

    @Value("\${scan.cache.size:10000}")
    private lateinit var scanCacheSize: String

    @Value("\${data.table.name}")
    private lateinit var dataTableName: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseReader::class.toString())
    }
}
