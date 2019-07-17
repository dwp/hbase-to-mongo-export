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
class HBaseReader constructor(private val connection: Connection): ItemReader<SourceRecord> {

    override fun read(): SourceRecord? {
        return scanner().next()?.let { result ->
            val value = result.getValue("cf".toByteArray(), "data".toByteArray())
            result.advance()
            val current = result.current()
            val timestamp = current.timestamp
            val json = value.toString(Charset.defaultCharset())
            val dataBlock = Gson().fromJson(json, JsonObject::class.java)
            val id = dataBlock.getAsJsonPrimitive("id").asString
            val encryptionInfo = dataBlock.getAsJsonObject("encryption")
            val encryptedKey = encryptionInfo.getAsJsonPrimitive("encryptedEncryptionKey").asString
            val encryptionKeyId = encryptionInfo.getAsJsonPrimitive("keyEncryptionKeyId").asString
            val initializationVector = encryptionInfo.getAsJsonPrimitive("initialisationVector").asString
            val encryptedDbObject = dataBlock.getAsJsonPrimitive("dbObject")?.asString
            if (encryptedDbObject.isNullOrEmpty()) {
                logger.error("'$id' missing dbObject field, skipping this record.")
                throw MissingFieldException(id, "dbObject")
            }
            val encryptionBlock = EncryptionBlock(encryptionKeyId, initializationVector, encryptedKey)
            return SourceRecord(id, timestamp, encryptionBlock, encryptedDbObject)
        }
    }

    fun resetScanner() {
        scanner = null
    }

    @Synchronized
    fun scanner(): ResultScanner {
        if (scanner == null) {
            logger.info("Getting '$tableName' table from '$connection'.")
            val table = connection.getTable(TableName.valueOf(tableName))
            val scan = Scan()
            scanner = table.getScanner(scan)
        }
        return scanner!!
    }

    private var scanner: ResultScanner? = null

    @Value("\${source.table.name}")
    private lateinit var tableName: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseReader::class.toString())
    }

}