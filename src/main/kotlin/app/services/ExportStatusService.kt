package app.services

interface ExportStatusService {
    fun incrementExportedCount(exportedFile: String)
    fun exportedFilesCount(): Int
    fun exportCompletionStatus(): ExportCompletionStatus
    fun setExportedStatus()
    fun setFailedStatus()
    fun setTableUnavailableStatus()
    fun setBlockedTopicStatus()
}
