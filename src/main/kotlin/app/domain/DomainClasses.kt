package app.domain

import com.google.gson.JsonObject
import org.apache.commons.text.StringEscapeUtils
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.BufferedOutputStream
import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.File

data class EncryptionBlock(val keyEncryptionKeyId: String,
                           val initializationVector: String,
                           val encryptedEncryptionKey: String)

data class EncryptionResult(val initialisationVector: String, val encrypted: String)

data class DataKeyResult(val dataKeyEncryptionKeyId: String, val plaintextDataKey: String, val ciphertextDataKey: String)

data class SourceRecord(val hbaseRowId: ByteArray,
                        val encryption: EncryptionBlock,
                        var dbObject: String,
                        val timestamp: Long,
                        var db: String,
                        var collection: String,
                        val outerType: String,
                        val innerType: String) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SourceRecord

        if (!hbaseRowId.contentEquals(other.hbaseRowId)) return false
        if (encryption != other.encryption) return false
        if (dbObject != other.dbObject) return false
        if (db != other.db) return false
        if (collection != other.collection) return false
        if (outerType != other.outerType) return false
        if (innerType != other.innerType) return false
        return true
    }

    override fun hashCode(): Int {
        var result = hbaseRowId.contentHashCode()
        result = 31 * result + encryption.hashCode()
        result = 31 * result + dbObject.hashCode()
        result = 31 * result + db.hashCode()
        result = 31 * result + collection.hashCode()
        result = 31 * result + outerType.hashCode()
        result = 31 * result + innerType.hashCode()
        return result
    }
}

data class DecryptedRecord(val dbObject: JsonObject, val manifestRecord: ManifestRecord)

data class ManifestRecord(val id: String, val timestamp: Long, val db: String, val collection: String,
                          val source: String, val externalOuterSource: String, val externalInnerSource: String,
                          val originalId: String)

data class Record(val dbObjectAsString: String, val manifestRecord: ManifestRecord)

data class EncryptingOutputStream(private val outputStream: BufferedOutputStream,
                                  val target: ByteArrayOutputStream,
                                  val dataKeyResult: DataKeyResult,
                                  val initialisationVector: String,
                                  val manifestFile: File,
                                  private val manifestWriter: BufferedWriter) {
    fun write(data: ByteArray) = outputStream.write(data)
    fun data(): ByteArray = target.toByteArray()

    fun close(): Boolean =
        try {
            outputStream.close()
            manifestWriter.close()
            true
        } catch (e: Exception) {
            logger.error("Failed to close output streams", e, "manifest_file" to "$manifestFile")
            false
        }

    fun writeManifestRecord(manifestRecord: ManifestRecord) = manifestWriter.write(csv(manifestRecord))

    private fun csv(manifestRecord: ManifestRecord) =
            "${escape(manifestRecord.id)}|${escape(manifestRecord.timestamp.toString())}|${escape(manifestRecord.db)}|${escape(manifestRecord.collection)}|${escape(manifestRecord.source)}|${escape(manifestRecord.externalOuterSource)}|${escape(manifestRecord.originalId)}|${escape(manifestRecord.externalInnerSource)}\n"

    private fun escape(value: String) = StringEscapeUtils.escapeCsv(value)
    private val logger = DataworksLogger.getLogger(EncryptingOutputStream::class)
}
