package app.domain

data class EncryptionBlock (val keyEncryptionKeyId: String,
                            val initializationVector: String,
                            val encryptedEncryptionKey: String)

data class EncryptionResult(val initialisationVector: String, val encrypted: String)

data class DataKeyResult(val dataKeyEncryptionKeyId: String, val plaintextDataKey: String, val ciphertextDataKey: String)

data class SourceRecord(val hbaseRowId: ByteArray,
                        val hbaseTimestamp: Long,
                        val encryption: EncryptionBlock,
                        var dbObject: String) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SourceRecord

        if (!hbaseRowId.contentEquals(other.hbaseRowId)) return false
        if (hbaseTimestamp != other.hbaseTimestamp) return false
        if (encryption != other.encryption) return false
        if (dbObject != other.dbObject) return false

        return true
    }

    override fun hashCode(): Int {
        var result = hbaseRowId.contentHashCode()
        result = 31 * result + hbaseTimestamp.hashCode()
        result = 31 * result + encryption.hashCode()
        result = 31 * result + dbObject.hashCode()
        return result
    }
}