package app.utils

import org.junit.Assert.*
import org.junit.Test

class TextUtilsTest {

    @Test
    fun testAllCharactersOk() {
        testValid("database","collection")
    }

    @Test
    fun testDigitsOk() {
        testValid("database1","collection1")
    }

    @Test
    fun testHyphensOk() {
        testValid("database-1","collection-1")
    }

    @Test
    fun testUnderscoresOk() {
        testValid("database_1","collection_1")
    }

    @Test
    fun testNonAlphaNonDashNotOkInDatabase() {
        testInvalid("database_1 ","collection_1")
    }

    @Test
    fun testNonAlphaNonDashNotOkInCollection() {
        testInvalid("database_1 ","collection_1!")
    }

    private fun testValid(database: String, collection: String) {
        val allChars = "ab.$database.$collection"
        val matcher = TextUtils().topicNameTableMatcher(allChars)
        assertNotNull(matcher)
        if (matcher != null) {
            val actualDatabase = matcher.groupValues[1]
            val actualCollection = matcher.groupValues[2]
            assertEquals(database, actualDatabase)
            assertEquals(collection, actualCollection)
        }
    }

    private fun testInvalid(database: String, collection: String) {
        val allChars = "ab.$database.$collection"
        val matcher = TextUtils().topicNameTableMatcher(allChars)
        assertNull(matcher)
    }
}
