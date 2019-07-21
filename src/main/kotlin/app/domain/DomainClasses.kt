package app.domain

data class EncryptionBlock (val encryptionKeyId: String,
                            val initializationVector: String,
                            val encryptedEncryptionKey: String)

data class EncryptionResult(val initialisationVector: String, val encrypted: String)

data class DataKeyResult(val masterKeyId: String, val plaintext: String, val cipherText: String)

data class SourceRecord(val hbaseId: String,
                        val hbaseTimestamp: Long,
                        val encryption: EncryptionBlock,
                        var dbObject: String)