package uk.gov.dwp.dataworks.export.domain

data class EncryptionBlock (val encryptionKeyId: String,
                            val encryptedEncryptionKey: String)
