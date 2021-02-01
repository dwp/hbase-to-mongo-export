package app.services

enum class ExportCompletionStatus(val description: String) {
    COMPLETED_SUCCESSFULLY("completed successfully"),
    COMPLETED_UNSUCCESSFULLY("failed"),
    NOT_COMPLETED("in progress")
}
