package app.utils

import io.kotest.matchers.shouldBe
import org.junit.Test

class IdUtilityTest {

    @Test
    fun unaltered() {
        val hbase = """{ "citizenId": "123455" }"""
        val (original, updated) = IdUtility.reverseEngineerId(hbase)
        original shouldBe """{"citizenId":"123455"}"""
        updated shouldBe original
    }

    @Test
    fun sorted() {
        val hbase = """{ "z": "12345", "a": "54321" }"""
        val (original, updated) = IdUtility.reverseEngineerId(hbase)
        original shouldBe """{"a":"54321","z":"12345"}"""
        updated shouldBe original
    }

    @Test
    fun altered() {
        val hbase = """{ "id": "123455" }"""
        val (original, updated) = IdUtility.reverseEngineerId(hbase)
        original shouldBe "123455"
        updated shouldBe """{"${'$'}oid":"123455"}"""
    }
}
