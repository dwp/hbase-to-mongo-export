package app.utils

import app.exceptions.BlockedTopicException
import org.junit.Test
import org.springframework.test.util.ReflectionTestUtils

class FilterBlockedTopicsUtilsTest {

    @Test
    fun shouldNotThrowExceptionWhenBlockedTopicsNotSet() {
        val topic = "some.topic"
        val util = FilterBlockedTopicsUtils()
        util.isTopicBlocked(topic)
    }

    @Test
    fun shouldNotThrowExceptionWhenTopicDoesNotMatchBlockedTopicList() {
        val topic = "good.topic"
        val blockedTopic = "blocked.topic,another.blocked.topic"
        val util = FilterBlockedTopicsUtils()
        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)
        util.isTopicBlocked(topic)
    }

    @Test
    fun shouldNotThrowExceptionWhenTopicIsADifferentCaseToABlockedTopicForSingleBlockedTopic() {
        val topic = "topic.string"
        val blockedTopic = "Topic.string"
        val util = FilterBlockedTopicsUtils()
        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)
        util.isTopicBlocked(topic)
    }

    @Test
    fun shouldNotThrowExceptionWhenTopicIsADifferentCaseToABlockedTopicForMultipleBlockedTopics() {
        val topic = "topic.String"
        val blockedTopic = "topic.string,another.topic.string"
        val util = FilterBlockedTopicsUtils()
        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)
        util.isTopicBlocked(topic)
    }

    @Test
    fun shouldNotThrowExceptionWhenTopicIsASubstringOfABlockedTopicForSingleBlockedTopic() {
        val topic = "topic.string"
        val blockedTopic = "topic.string.full"
        val util = FilterBlockedTopicsUtils()
        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)
        util.isTopicBlocked(topic)
    }

    @Test
    fun shouldNotThrowExceptionWhenTopicIsASubstringOfABlockedTopicForMultipleBlockedTopic() {
        val topic = "topic.string"
        val blockedTopic = "blocked.topic,topic.string.full"
        val util = FilterBlockedTopicsUtils()
        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)
        util.isTopicBlocked(topic)
    }

    @Test(expected = BlockedTopicException::class)
    fun shouldThrowBlockedTopicExceptionWhenTopicIsInSingularBlockedList() {
        val topic = "blocked.topic"
        val blockedTopic = "blocked.topic"
        val util = FilterBlockedTopicsUtils()
        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)
        util.isTopicBlocked(topic)
    }

    @Test(expected = BlockedTopicException::class)
    fun shouldThrowBlockedTopicExceptionWhenTopicIsInBlockedList() {
        val topic = "blocked.topic"
        val blockedTopic = "blocked.topic,another.blocked.topic"
        val util = FilterBlockedTopicsUtils()
        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)
        util.isTopicBlocked(topic)
    }
}
