package app.services

interface ExportStatusService {
    fun incrementExportedCount(exportedFile: String)
    fun setCollectionStatus(status: String)
}
