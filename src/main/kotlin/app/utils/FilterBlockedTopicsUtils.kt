package app.utils

import app.exceptions.BlockedTopicException
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class FilterBlockedTopicsUtils {

    @Value("\${blocked.topics:NOT_SET}")
    lateinit var blockedTopics: String

    @Throws(BlockedTopicException::class)
    fun isTopicBlocked(topic: String) {
        if (blockedTopics.contains(topic))
            throw BlockedTopicException(topic)
    }
}
