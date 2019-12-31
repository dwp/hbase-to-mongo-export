package app.utils

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.CoreConstants
import ch.qos.logback.core.LayoutBase;

class LoggerLayoutAppender : LayoutBase<ILoggingEvent>() {

    override fun doLayout(event: ILoggingEvent?): String {
        /*
        StringBuffer sbuf = new StringBuffer(128);
        sbuf.append(event.getTimeStamp() - event.getLoggingContextVO.getBirthTime());
        sbuf.append(" ");
        sbuf.append(event.getLevel());
        sbuf.append(" [");
        sbuf.append(event.getThreadName());
        sbuf.append("] ");
        sbuf.append(event.getLoggerName();
        sbuf.append(" - ");
        sbuf.append(event.getFormattedMessage());
        sbuf.append(CoreConstants.LINE_SEP);
        return sbuf.toString();
         */
        if (event == null) {
            return ""
        }
        //val result = "{ timestamp:\"%d{HH:mm:ss.SSS}\", thread:\"%thread\", log_level:\"%level\", logger:\"%logger{32}\", application:\"HTME\", message:\"%msg\" }%n</pattern>"
        val result = "{ timestamp:\"${event.timeStamp}\", thread:\"${event.threadName}\", log_level:\"${event.level}\", logger:\"${event.loggerName}\", application:\"HTME\", message:\"${event.formattedMessage}\" }"
        return result + CoreConstants.LINE_SEPARATOR
    }

}
