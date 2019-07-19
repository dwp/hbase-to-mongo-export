package app.domain

data class SourceRecord(val hbaseId: String,
                        val hbaseTimestamp: Long,
                        val encryption: EncryptionBlock,
                        var dbObject: String)