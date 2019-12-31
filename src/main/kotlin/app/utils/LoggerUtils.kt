package app.utils

/**
This file had simple json logging utils extending SLF4J in a standard way.
It brings all our changes into one code module and one test file, and allows us to make a simple override in the
resources/logback.xml file so that all vagaries are in code and unit-testable; one "just" does this:

<appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
  <encoder class="ch.qos.logback.core.encoder.LayoutWrappingEncoder">
    <layout class="app.utils.LoggerLayoutAppender" />
  </encoder>
</appender>

See http://logback.qos.ch/manual/layouts.html for examples.
 */

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.CoreConstants
import ch.qos.logback.core.LayoutBase
import org.apache.commons.text.StringEscapeUtils
import org.slf4j.Logger
import java.text.SimpleDateFormat
import java.util.*

private val defaultFormat = SimpleDateFormat("HH:mm:ss.SSS")

fun logDebug(logger: Logger, message: String, vararg tuples: String) {
    val semiFormatted = semiFormattedTuples(message, *tuples)
    logger.debug(semiFormatted)
}

fun logInfo(logger: Logger, message: String, vararg tuples: String) {
    val semiFormatted = semiFormattedTuples(message, *tuples)
    logger.info(semiFormatted)
}

fun logError(logger: Logger, message: String, vararg tuples: String) {
    val semiFormatted = semiFormattedTuples(message, *tuples)
    logger.error(semiFormatted)
}

fun logError(logger: Logger, message: String, error: Throwable, vararg tuples: String) {
    val semiFormatted = semiFormattedTuples(message, *tuples)
    logger.error(semiFormatted, error)
}

fun semiFormattedTuples(message: String, vararg tuples: String): String {
    var semiFormatted = StringEscapeUtils.escapeJson(message)
    if (tuples.isEmpty()) {
        return semiFormatted
    }
    if (tuples.size % 2 != 0) {
        throw IllegalArgumentException("Must have matched key-value pairs but had ${tuples.size} argument(s)")
    }
    for (i in tuples.indices step 2) {
        val key = tuples[i]
        val value = tuples[i + 1]
        val escapedValue = StringEscapeUtils.escapeJson(value)
        semiFormatted += "\", \"$key\":\"$escapedValue"
    }
    return semiFormatted
}

fun formattedTimestamp(epochTime: Long): String {
    try {
        synchronized(defaultFormat) {
            val netDate = Date(epochTime)
            return defaultFormat.format(netDate)
        }
    } catch (e: Exception) {
        throw e
    }
}

class LoggerLayoutAppender : LayoutBase<ILoggingEvent>() {

    override fun doLayout(event: ILoggingEvent?): String {
        if (event == null) {
            return ""
        }
        val dateTime = formattedTimestamp(event.timeStamp)
        val result = "{ timestamp:\"$dateTime\", thread:\"${event.threadName}\", log_level:\"${event.level}\", logger:\"${event.loggerName}\", application:\"HTME\", message:\"${event.formattedMessage}\" }"
        return result + CoreConstants.LINE_SEPARATOR
    }

}

