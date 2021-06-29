package app.services

enum class ExportCompletionStatus(val description: String) {
    COMPLETED_SUCCESSFULLY("success"),
    COMPLETED_UNSUCCESSFULLY("failed"),
    IN_PROGRESS("in progress")
    NOT_COMPLETED("not completed")
}
