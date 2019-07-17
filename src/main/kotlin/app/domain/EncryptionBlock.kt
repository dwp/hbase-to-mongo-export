package app.domain

data class EncryptionBlock (val encryptionKeyId: String,
                            val initializationVector: String,
                            val encryptedEncryptionKey: String)
