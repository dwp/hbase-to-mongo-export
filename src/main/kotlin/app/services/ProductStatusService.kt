package app.services

interface ProductStatusService {
    fun setCompletedStatus()
    fun setFailedStatus()
}
