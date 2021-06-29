package app.services

import app.services.ExportCompletionStatus

interface SnsService {
    fun sendExportCompletedSuccessfullyMessage()
    fun sendTopicFailedMonitoringMessage()
    fun sendCompletionMonitoringMessage(completionStatus: ExportCompletionStatus)
}
