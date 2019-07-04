package uk.gov.dwp.dataworks.export.domain

data class SourceRecord(val _id: RecordId,
                        val _lastModifiedDateTime: String,
                        val encryption: EncryptionBlock,
                        var dbObject: String)