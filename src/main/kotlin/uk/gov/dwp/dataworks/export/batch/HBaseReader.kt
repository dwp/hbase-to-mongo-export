package uk.gov.dwp.dataworks.export.batch

import com.google.gson.Gson
import com.google.gson.JsonObject
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.export.domain.EncryptionBlock
import uk.gov.dwp.dataworks.export.domain.RecordId
import uk.gov.dwp.dataworks.export.domain.SourceRecord
import uk.gov.dwp.dataworks.export.exceptions.MissingFieldException
import java.nio.charset.Charset

@Component
class HBaseReader
@Autowired
constructor(private val connection: Connection): ItemReader<SourceRecord> {

    override fun read(): SourceRecord? {
        return scanner().next()?.let {
            val value = it.getValue("cf".toByteArray(), "data".toByteArray())
            val json = value.toString(Charset.defaultCharset())
            logger.info("json: '$json'.")
            val dataBlock = Gson().fromJson(json, JsonObject::class.java)
            val id = dataBlock.getAsJsonPrimitive("id").asString
            val encryptionInfo = dataBlock.getAsJsonObject("encryption")
            val encryptedKey = encryptionInfo.getAsJsonPrimitive("encryptedEncryptionKey").asString
            val encryptionKeyId = encryptionInfo.getAsJsonPrimitive("keyEncryptionKeyId").asString
            val encryptedDbObject = dataBlock.getAsJsonPrimitive("dbObject")?.asString

            logger.info("encryptedDbObject: '$encryptedDbObject'.")
            if (encryptedDbObject.isNullOrEmpty()) {
                throw MissingFieldException(id,  "dbObject")
            }
            val recordId = RecordId(id)
            val lastModified = dataBlock.getAsJsonPrimitive("timestamp").asString
            val encryptionBlock = EncryptionBlock(encryptionKeyId, encryptedKey)
            return SourceRecord(recordId, lastModified, encryptionBlock, encryptedDbObject!!)
        }
    }

    fun resetScanner() {
        scanner = null
    }

    fun scanner(): ResultScanner {
        if (scanner == null) {
            logger.info("Getting table from '$connection'.")
            val table = connection.getTable(TableName.valueOf("ucdata"))
            val scan = Scan()
            scanner = table.getScanner(scan)
        }
        return scanner!!
    }

    private var scanner: ResultScanner? = null

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseReader::class.toString())
    }

}