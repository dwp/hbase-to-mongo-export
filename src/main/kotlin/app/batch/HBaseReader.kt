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

@Component
class HBaseReader constructor(private val connection: Connection) : ItemReader<SourceRecord> {

    override fun read(): SourceRecord? {
        return scanner().next()?.let { result ->

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
            return SourceRecord(idBytes, timestamp, encryptionBlock, encryptedDbObject, db, collection)
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

            scanner = table.getScanner(scan)
        }
        return scanner!!
    }

    private var scanner: ResultScanner? = null

    @Value("\${column.family}")
    private lateinit var columnFamily: String // i.e. "topic"

    @Value("\${topic.name}")
    private lateinit var topicName: String // i.e. "db.user.data"

    @Value("\${data.table.name}")
    private lateinit var dataTableName: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseReader::class.toString())
    }
}
