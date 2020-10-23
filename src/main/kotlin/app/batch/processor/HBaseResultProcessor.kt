package app.batch.processor

import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.MissingFieldException
import com.google.gson.Gson
import com.google.gson.JsonObject
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.nio.charset.Charset

@Component
class HBaseResultProcessor : ItemProcessor<Result, SourceRecord> {

    override fun process(result: Result): SourceRecord? {
        val idBytes = result.row
        val value = result.value()
        val json = value.toString(Charset.defaultCharset())
        val dataBlock = Gson().fromJson(json, JsonObject::class.java)
        val outerType = dataBlock.getAsJsonPrimitive("@type")?.asString ?: ""
        val messageInfo = dataBlock.getAsJsonObject("message")
        val innerType = messageInfo.getAsJsonPrimitive("@type")?.asString ?: ""
        val encryptedDbObject = messageInfo.getAsJsonPrimitive("dbObject")?.asString
        val db = messageInfo.getAsJsonPrimitive("db")?.asString
        val collection = messageInfo.getAsJsonPrimitive("collection")?.asString
        val encryptionInfo = messageInfo.getAsJsonObject("encryption")
        val encryptedEncryptionKey = encryptionInfo.getAsJsonPrimitive("encryptedEncryptionKey").asString
        val keyEncryptionKeyId = encryptionInfo.getAsJsonPrimitive("keyEncryptionKeyId").asString
        val initializationVector = encryptionInfo.getAsJsonPrimitive("initialisationVector").asString
        validateMandatoryField(encryptedDbObject, idBytes)
        validateMandatoryField(db, idBytes)
        validateMandatoryField(collection, idBytes)
        val encryptionBlock = EncryptionBlock(keyEncryptionKeyId, initializationVector, encryptedEncryptionKey)
        return SourceRecord(idBytes, encryptionBlock, encryptedDbObject!!, timestamp(result), db!!, collection!!,
                if (StringUtils.isNotBlank(outerType)) outerType else "TYPE_NOT_SET",
                if (StringUtils.isNotBlank(innerType)) innerType else "TYPE_NOT_SET")
    }

    private fun timestamp(result: Result): Long =
        result.getColumnLatestCell(columnFamily, columnQualifier).let {
            it.timestamp
        }


    private fun validateMandatoryField(mandatoryFieldValue: String?, idBytes: ByteArray) {
        if (mandatoryFieldValue.isNullOrEmpty()) {
            logger.error("Missing dbObject field, skipping this record", "id_bytes" to "$idBytes")
            throw MissingFieldException(idBytes, "dbObject")
        }
    }

    companion object {
        private val logger = DataworksLogger.getLogger(HBaseResultProcessor::class.java.toString())
        private val columnFamily = Bytes.toBytes("cf")
        private val columnQualifier = Bytes.toBytes("record")
    }
}
