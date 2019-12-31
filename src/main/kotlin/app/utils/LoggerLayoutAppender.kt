package app.utils

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.CoreConstants
import ch.qos.logback.core.LayoutBase;
import java.text.SimpleDateFormat
import java.util.*

class LoggerLayoutAppender : LayoutBase<ILoggingEvent>() {

    private val sdf = SimpleDateFormat("HH:mm:ss.SSS")

    override fun doLayout(event: ILoggingEvent?): String {
        if (event == null) {
            return ""
        }
        val dateTime = getDateTime(event.timeStamp)
        val result = "{ timestamp:\"$dateTime\", thread:\"${event.threadName}\", log_level:\"${event.level}\", logger:\"${event.loggerName}\", application:\"HTME\", message:\"${event.formattedMessage}\" }"
        return result + CoreConstants.LINE_SEPARATOR
    }

    fun getDateTime(epocTime: Long): String {
        try {
            val netDate = Date(epocTime)
            return sdf.format(netDate)
        } catch (e: Exception) {
            throw e
        }
    }
}
