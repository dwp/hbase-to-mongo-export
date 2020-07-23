package app.utils

import app.exceptions.TopicIsBlockedException
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class FilterBlockedTopicsUtils {

    @Value("\${blocked.topics:NOT_SET}")
    lateinit var blockedTopics: String

    @Throws(TopicIsBlockedException::class)
    fun isTopicBlocked(topic: String) {
        if (blockedTopics.contains(topic))
            throw TopicIsBlockedException(topic)
    }
}
