package app.utils.logging

/**
This file had simple json logging utils extending SLF4J in a standard way.
It brings all our changes into one code module and one test file, and allows us to make a simple override in the
resources/logback.xml file so that all vagaries are in code and unit-testable; one "just" does this:

<appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
<encoder class="ch.qos.logback.core.encoder.LayoutWrappingEncoder">
<layout class="app.utils.logging.LoggerLayoutAppender" />
</encoder>
</appender>

See http://logback.qos.ch/manual/layouts.html for examples.
 */

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.ThrowableProxyUtil
import ch.qos.logback.core.CoreConstants
import ch.qos.logback.core.LayoutBase
import org.apache.commons.text.StringEscapeUtils
import org.slf4j.Logger
import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.*


private val defaultFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS") // 2001-07-04T12:08:56.235
private var hostname = InetAddress.getLocalHost().hostName
private var environment = System.getProperty("environment", "NOT_SET")
private var application = System.getProperty("application", "NOT_SET")
private var app_version = System.getProperty("app_version", "NOT_SET")
private var component = System.getProperty("component", "NOT_SET")

fun useLoggerDefaultValues() {
    hostname = InetAddress.getLocalHost().hostName
    environment = System.getProperty("environment", "NOT_SET")
    application = System.getProperty("application", "NOT_SET")
    app_version = System.getProperty("app_version", "NOT_SET")
    component = System.getProperty("component", "NOT_SET")
}

fun testOverrides(host: String, env: String, app: String, version: String, comp: String) {
    hostname = host
    environment = env
    application = app
    app_version = version
    component = comp
}

fun logDebug(logger: Logger, message: String, vararg tuples: String) {
    val semiFormatted = semiFormattedTuples(message, *tuples)
    logger.debug(semiFormatted)
}

fun logInfo(logger: Logger, message: String, vararg tuples: String) {
    val semiFormatted = semiFormattedTuples(message, *tuples)
    logger.info(semiFormatted)
}

fun logWarn(logger: Logger, message: String, vararg tuples: String) {
    val semiFormatted = semiFormattedTuples(message, *tuples)
    logger.warn(semiFormatted)
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

fun inlineStackTrace(fullTrace: String): String {
    return try {
        StringEscapeUtils.escapeJson(fullTrace.replace("\n", " | ").replace("\t", " "))
    } catch (ex: java.lang.Exception) {
        fullTrace
    }
}

fun throwableProxyEventToString(event: ILoggingEvent): String {
    val throwableProxy = event.throwableProxy
    if (throwableProxy != null) {
        val throwableStr = ThrowableProxyUtil.asString(throwableProxy)
        val inlined = inlineStackTrace(throwableStr)
        return "\"exception\"=\"$inlined\", "
    }
    return ""
}

class LoggerLayoutAppender : LayoutBase<ILoggingEvent>() {

    private val staticData = "\"hostname\"=\"$hostname\", " +
        "\"environment\"=\"$environment\", " +
        "\"application\"=\"$application\", " +
        "\"app_version\"=\"$app_version\", " +
        "\"component\"=\"$component\""

    override fun doLayout(event: ILoggingEvent?): String {
        if (event == null) {
            return ""
        }
        val dateTime = formattedTimestamp(event.timeStamp)
        val resultPrefix = "{ " +
            "\"timestamp\":\"$dateTime\", " +
            "\"thread\":\"${event.threadName}\", " +
            "\"log_level\":\"${event.level}\", " +
            "\"logger\":\"${event.loggerName}\", "
        val resultSuffix = staticData +
            " }" + CoreConstants.LINE_SEPARATOR
        val messageResult = "\"message\":\"${event.formattedMessage}\", "
        val exceptionResult = throwableProxyEventToString(event)

        return resultPrefix + messageResult + exceptionResult + resultSuffix
    }
}

