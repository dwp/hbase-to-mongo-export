package app.utils

import app.batch.HBaseReaderTest
import app.exceptions.BlockedTopicException
import io.kotest.assertions.throwables.shouldNotThrow
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.test.TestResult.Companion.success
import io.kotest.fp.success
import io.kotest.matchers.shouldBe
import org.junit.Test
import org.springframework.test.util.ReflectionTestUtils

class FilterBlockedTopicsUtilsTest {

    @Test
    fun shouldNotThrowExceptionWhenBlockedTopicsNotSet() {

        val topic = "some.topic"

        val util = FilterBlockedTopicsUtils()

        val exception = shouldNotThrow<BlockedTopicException> {
            util.isTopicBlocked(topic)
            success()
        }
        assert(exception.success().isSuccess())
    }

    @Test
    fun shouldNotThrowExceptionWhenTopicDoesNotMatchBlockedTopicList() {

        val topic = "good.topic"

        val blockedTopic = "blocked.topic,another.blocked.topic"

        val util = FilterBlockedTopicsUtils()

        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)

        val exception = shouldNotThrow<BlockedTopicException> {
            util.isTopicBlocked(topic)
            success()
        }
        assert(exception.success().isSuccess())
    }

    @Test
    fun shouldNotThrowExceptionWhenTopicIsADifferentCaseToABlockedTopicForSingleBlockedTopic() {

        val topic = "topic.string"

        val blockedTopic = "Topic.string"

        val util = FilterBlockedTopicsUtils()

        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)

        val exception = shouldNotThrow<BlockedTopicException> {
            util.isTopicBlocked(topic)
            success()
        }
        assert(exception.success().isSuccess())
    }

    @Test
    fun shouldNotThrowExceptionWhenTopicIsADifferentCaseToABlockedTopicForMultipleBlockedTopics() {

        val topic = "topic.String"

        val blockedTopic = "topic.string,another.topic.string"

        val util = FilterBlockedTopicsUtils()

        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)

        val exception = shouldNotThrow<BlockedTopicException> {
            util.isTopicBlocked(topic)
            success()
        }
        assert(exception.success().isSuccess())
    }

    @Test
    fun shouldNotThrowExceptionWhenTopicIsASubstringOfABlockedTopicForSingleBlockedTopic() {

        val topic = "topic.string"

        val blockedTopic = "topic.string.full"

        val util = FilterBlockedTopicsUtils()

        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)

        val exception = shouldNotThrow<BlockedTopicException> {
            util.isTopicBlocked(topic)
            success()
        }
        assert(exception.success().isSuccess())
    }

    @Test
    fun shouldNotThrowExceptionWhenTopicIsASubstringOfABlockedTopicForMultipleBlockedTopic() {

        val topic = "topic.string"

        val blockedTopic = "blocked.topic,topic.string.full"

        val util = FilterBlockedTopicsUtils()

        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)

        val exception = shouldNotThrow<BlockedTopicException> {
            util.isTopicBlocked(topic)
            success()
        }
        assert(exception.success().isSuccess())
    }

    @Test
    fun shouldThrowBlockedTopicExceptionWhenTopicIsInSingularBlockedList() {

        val topic = "blocked.topic"

        val blockedTopic = "blocked.topic"

        val util = FilterBlockedTopicsUtils()

        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)

        val exception = shouldThrow<BlockedTopicException> {
            util.isTopicBlocked(topic)
        }
        exception.message shouldBe "Provided topic is blocked so cannot be processed: 'blocked.topic'"
    }

    @Test
    fun shouldThrowBlockedTopicExceptionWhenTopicIsInBlockedList() {

        val topic = "blocked.topic"

        val blockedTopic = "blocked.topic,another.blocked.topic"

        val util = FilterBlockedTopicsUtils()

        ReflectionTestUtils.setField(util, "blockedTopics", blockedTopic)

        val exception = shouldThrow<BlockedTopicException> {
            util.isTopicBlocked(topic)
        }
        exception.message shouldBe "Provided topic is blocked so cannot be processed: 'blocked.topic'"
    }
}
