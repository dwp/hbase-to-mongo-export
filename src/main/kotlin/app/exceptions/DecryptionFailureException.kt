package app.exceptions

class DecryptionFailureException (database: String,
                                  collection: String,
                                  id: String,
                                  timestamp: Long,
                                  masterKeyId: String,
                                  cause: Throwable): Exception("""
                                      Failed to decrypt record '$id', timestamp '$timestamp' sourced from
                                      collection '$collection' on database '$database', master key id: '$masterKeyId'.
                                  """.trimIndent().replace('\n', ' '), cause)