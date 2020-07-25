package app.utils

import app.batch.HBaseReaderTest
import app.exceptions.BlockedTopicException
import arrow.core.success
import io.kotlintest.matchers.beEmpty
import io.kotlintest.matchers.types.beNull
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotThrow
import io.kotlintest.shouldThrow
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
