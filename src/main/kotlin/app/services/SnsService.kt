package app.services

interface SnsService {
    fun sendExportCompletedMessage()
    fun sendMonitoringMessage()
}
