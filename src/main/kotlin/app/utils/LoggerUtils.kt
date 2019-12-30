package app.utils

import org.slf4j.Logger

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
    var semiFormatted = message
    if (tuples.isEmpty()) {
        return semiFormatted
    }
    if (tuples.size % 2 != 0) {
        throw IllegalArgumentException("Must have matched key-value pairs but had ${tuples.size} argument(s)")
    }
    for (i in tuples.indices step 2) {
        val key = tuples[i]
        val value = tuples[i + 1]
        semiFormatted += "\", \"$key\":\"$value"
    }
    return semiFormatted
}
