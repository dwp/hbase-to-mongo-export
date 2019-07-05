package app.domain

data class SourceRecord(val _id: RecordId,
                        val _lastModifiedDateTime: String,
                        val encryption: EncryptionBlock,
                        var dbObject: String)