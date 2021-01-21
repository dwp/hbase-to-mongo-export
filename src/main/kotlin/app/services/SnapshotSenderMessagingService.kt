package app.services

interface SnapshotSenderMessagingService {
    fun notifySnapshotSender(prefix: String)
    fun notifySnapshotSenderNoFilesExported()
}
