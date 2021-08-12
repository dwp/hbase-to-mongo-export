package app.batch

import app.domain.ManifestRecord
import app.domain.Record
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.test.util.ReflectionTestUtils

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [TransformationProcessor::class])
class TransformationProcessorTest {

    @Test
    fun transformsEqualitiesRecord() {
        ReflectionTestUtils.setField(processor, "topicName", "data.equality")
        val manifestRecord = ManifestRecord("id", 1L, "data", "equality", "source",
            "externalOuterSource", "externalInnerSource", "originalId")
        val item = Record("""{ "_id": "12345" }""", manifestRecord)
        val actual = processor.process(item)
        assertNotNull(actual)
        assertEquals("""{"message":{"_id":"12345","@type":"externalInnerSource"}}""", actual?.dbObjectAsString)
        assertSame(item.manifestRecord, actual?.manifestRecord)
    }

    @Test
    fun doesNotTransformNonEqualitiesRecord() {
        ReflectionTestUtils.setField(processor, "topicName", "db.database.collection")
        val manifestRecord = ManifestRecord("id", 1L, "database", "collection", "source",
            "externalOuterSource", "externalInnerSource", "originalId")
        val item = Record("""{ "_id": "12345" }""", manifestRecord)
        val actual = processor.process(item)
        assertSame(item, actual)
        assertSame(item.dbObjectAsString, actual?.dbObjectAsString)
    }

    @Autowired
    private lateinit var processor: TransformationProcessor
}
