package app.services

import app.domain.EncryptingOutputStream

interface S3ObjectService {
    fun putObject(objectKey: String, encryptingOutputStream: EncryptingOutputStream)
}
