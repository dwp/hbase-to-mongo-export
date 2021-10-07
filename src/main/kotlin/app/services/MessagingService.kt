package app.services

interface MessagingService {
    fun notifySnapshotSender(prefix: String)
    fun notifySnapshotSenderNoFilesExported()
    fun sendDataEgressMessage(prefix: String)
}
