package app.utils

import org.junit.Assert.*
import org.junit.Test

class TextUtilsTest {

    @Test
    fun testAllCharactersOk() {
        testValid("db.", "database","collection")
    }

    @Test
    fun testDigitsOk() {
        testValid("db.", "database1","collection1")
    }

    @Test
    fun testHyphensOk() {
        testValid("db.", "database-1","collection-1")
    }

    @Test
    fun testUnderscoresOk() {
        testValid("db.", "database_1","collection_1")
    }

    @Test
    fun testAllCharactersOkGivenNoPrefix() {
        testValid("", "database","collection")
    }

    @Test
    fun testDigitsOkGivenNoPrefix() {
        testValid("", "database1","collection1")
    }

    @Test
    fun testHyphensOkGivenNoPrefix() {
        testValid("", "database-1","collection-1")
    }

    @Test
    fun testUnderscoresOkGivenNoPrefix() {
        testValid("", "database_1","collection_1")
    }

    @Test
    fun testNonAlphaNonDashNotOkInDatabase() {
        testInvalid("test.", "database_1 ","collection_1")
    }

    @Test
    fun testNonAlphaNonDashNotOkInCollection() {
        testInvalid("db.", "database_1 ","collection_1!")
    }

    @Test
    fun testNonAlphaNonDashNotOkInDatabaseGivenNoPrefix() {
        testInvalid("", "database_1 ","collection_1")
    }

    @Test
    fun testNonAlphaNonDashNotOkInCollectionGivenNoPrefix() {
        testInvalid("", "database_1 ","collection_1!")
    }

    private fun testValid(prefix: String, database: String, collection: String) {
        val allChars = "$prefix$database.$collection"
        val matcher = TextUtils().topicNameTableMatcher(allChars)
        assertNotNull(matcher)
        if (matcher != null) {
            val actualDatabase = matcher.groupValues[1]
            val actualCollection = matcher.groupValues[2]
            assertEquals(database, actualDatabase)
            assertEquals(collection, actualCollection)
        }
    }

    private fun testInvalid(prefix: String, database: String, collection: String) {
        val allChars = "$prefix$database.$collection"
        val matcher = TextUtils().topicNameTableMatcher(allChars)
        assertNull(matcher)
    }
}
