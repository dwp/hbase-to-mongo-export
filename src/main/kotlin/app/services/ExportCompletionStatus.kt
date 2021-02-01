package app.services

enum class ExportCompletionStatus(val description: String) {
    COMPLETED_SUCCESSFULLY("Completed Successfully"),
    COMPLETED_UNSUCCESSFULLY("Failed"),
    NOT_COMPLETED("In progress")
}
