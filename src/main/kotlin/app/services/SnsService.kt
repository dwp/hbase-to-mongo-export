package app.services

import org.springframework.batch.core.ExitStatus

interface SnsService {
    fun sendExportCompletedSuccessfullyMessage()
    fun sendMonitoringMessage(exitStatus: ExitStatus)
}
