package app.exceptions

class MissingFieldException(id: ByteArray, field: String) :
        Exception("Missing field '$field' in record '$id'.")

class DecryptionFailureException(database: String,
                                 collection: String,
                                 id: ByteArray,
                                 timestamp: Long,
                                 masterKeyId: String,
                                 cause: Throwable) : Exception("""
                                      Failed to decrypt record '$id', timestamp '$timestamp' sourced from
                                      collection '$collection' on database '$database', master key id: '$masterKeyId'.
                                  """.trimIndent().replace('\n', ' '), cause)

class DataKeyDecryptionException(message: String) : Exception(message)

class DataKeyServiceUnavailableException(message: String) : Exception(message)

class BadDecryptedDataException(hbaseRowkey: String, db: String, collection: String, reason: String) : Exception("Exception in processing the decrypted record id '$hbaseRowkey' in db '$db' in collection '$collection' with the reason '$reason'")
