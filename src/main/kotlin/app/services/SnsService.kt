package app.services

interface SnsService {
    fun sendExportCompletedSuccessfullyMessage()
    fun sendMonitoringMessage(exitStatus: ExitStatus
}
