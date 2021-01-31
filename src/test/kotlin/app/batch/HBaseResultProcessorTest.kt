package app.batch

import app.batch.processor.HBaseResultProcessor
import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.MissingFieldException
import app.utils.TextUtils
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.sqs.AmazonSQS
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.reset
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.BDDMockito.given
import org.mockito.Mockito
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.test.util.ReflectionTestUtils
import java.nio.charset.Charset

@RunWith(SpringRunner::class)
@ActiveProfiles("phoneyCipherService", "phoneyDataKeyService", "unitTest", "outputToConsole")
@SpringBootTest
@TestPropertySource(properties = [
    "topic.name=db.a.b",
    "identity.keystore=resources/identity.jks",
    "trust.keystore=resources/truststore.jks",
    "identity.store.password=changeit",
    "identity.key.password=changeit",
    "trust.store.password=changeit",
    "identity.store.alias=cid",
    "hbase.zookeeper.quorum=hbase",
    "aws.region=eu-west-2",
    "s3.bucket=bucket",
    "s3.prefix.folder=prefix",
    "snapshot.sender.sqs.queue.url=http://aws:4566",
    "snapshot.sender.reprocess.files=true",
    "snapshot.sender.shutdown.flag=true",
    "snapshot.sender.export.date=2020-06-05",
    "trigger.snapshot.sender=false",
    "snapshot.type=full"
])
class HBaseResultProcessorTest {
    @Before
    fun setUp() {
        reset(result)
    }

    @Test
    fun testRead() {
        val commonBlock = commonBlock("database", "collection")
        val cellData = """
            |{
            |  "@type": "OUTER_TYPE",
            |  "message": {
            |   $commonBlock,
            |   "@type": "INNER_TYPE",
            |   "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |   "dbObject": "$dbObject"
            |  }
            |}""".trimMargin()
        init(cellData)
        assertResult(expectedResult("OUTER_TYPE", "INNER_TYPE", "database", "collection"), actualResult())
    }

    @Test
    fun testReadNoDb() {
        val commonBlock = commonBlock("", "collection")
        val cellData = """
            |{
            |  "@type": "OUTER_TYPE",
            |  "message": {
            |   $commonBlock,
            |   "@type": "INNER_TYPE",
            |   "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |   "dbObject": "$dbObject"
            |  }
            |}""".trimMargin()
        init(cellData)
        assertResult(expectedResult("OUTER_TYPE", "INNER_TYPE", "a", "collection"), actualResult())
    }

    @Test
    fun testReadNoCollection() {
        val commonBlock = commonBlock("database", "")
        val cellData = """
            |{
            |  "@type": "OUTER_TYPE",
            |  "message": {
            |   $commonBlock,
            |   "@type": "INNER_TYPE",
            |   "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |   "dbObject": "$dbObject"
            |  }
            |}""".trimMargin()
        init(cellData)
        assertResult(expectedResult("OUTER_TYPE", "INNER_TYPE", "database", "b"), actualResult())
    }

    @Test
    fun testReadNoDbOrCollection() {
        val commonBlock = commonBlock("", "")
        val cellData = """
            |{
            |  "@type": "OUTER_TYPE",
            |  "message": {
            |   $commonBlock,
            |   "@type": "INNER_TYPE",
            |   "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |   "dbObject": "$dbObject"
            |  }
            |}""".trimMargin()
        init(cellData)
        assertResult(expectedResult("OUTER_TYPE", "INNER_TYPE", "a", "b"), actualResult())
    }

    @Test
    fun testInnerTypeWhenNoOuterType() {
        val commonBlock = commonBlock("database", "collection")
        val cellData = """
            |{
            |  "message": {
            |   $commonBlock,
            |    "@type": "INNER_TYPE",
            |    "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |    "dbObject": "$dbObject"
            |  }
            |}""".trimMargin()

        init(cellData)
        assertResult(expectedResult("TYPE_NOT_SET", "INNER_TYPE", "database", "collection"), actualResult())
    }

    @Test
    fun testInnerTypeWhenEmptyOuterType() {
        val commonBlock = commonBlock("database", "collection")
        val cellData = """
            |{
            |  "@type": "",
            |  "message": {
            |   $commonBlock,
            |    "@type": "INNER_TYPE",
            |    "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |    "dbObject": "$dbObject"
            |  }
            |}""".trimMargin()
        init(cellData)
        assertResult(expectedResult("TYPE_NOT_SET", "INNER_TYPE", "database", "collection"), actualResult())
    }

    @Test
    fun testReadModifiedIsObject() {
        val commonBlock = commonBlock("database", "collection")
        val dateKey = "\$date"

        val lastModified = "2019-07-04T07:27:35.104+0000"
        val cellData = """
            |{
            |  "@type": "OUTER_TYPE",
            |  "message": {
            |   $commonBlock,
            |    "@type": "INNER_TYPE",
            |    "_lastModifiedDateTime": {
            |       $dateKey: "$lastModified"
            |    },
            |    "dbObject": "$dbObject"
            |  }
            |}""".trimMargin()

        init(cellData)
        assertResult(expectedResult("OUTER_TYPE", "INNER_TYPE", "database", "collection"), actualResult())
    }

    @Test
    fun testReadModifiedIsAbsent() {
        val commonBlock = commonBlock("database", "collection")
        val cellData = """
            |{
            |  "@type": "OUTER_TYPE",
            |  "message": {
            |   $commonBlock,
            |    "@type": "INNER_TYPE",
            |    "dbObject": "$dbObject"
            |  }
            |}""".trimMargin()

        init(cellData)
        assertResult(expectedResult("OUTER_TYPE", "INNER_TYPE", "database", "collection"), actualResult())
    }

    @Test
    fun testReadNoType() {
        val commonBlock = commonBlock("database", "collection")
        val cellData = """
            |{
            |  "message": {
            |   $commonBlock,
            |    "dbObject": "$dbObject"
            |  }
            |}""".trimMargin()

        init(cellData)
        assertResult(expectedResult("TYPE_NOT_SET","TYPE_NOT_SET", "database", "collection"), actualResult())
    }

    @Test(expected = MissingFieldException::class)
    fun testReject() {
        val commonBlock = commonBlock("database", "collection")
        val cellData = """
            |{
            |  "@type": "V4",
            |  "message": {
            |   $commonBlock,
            |    "@type": "MONGO_INSERT",
            |    "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000"
            |  }
            |}""".trimMargin()

        init(cellData)
        actualResult()
    }


    @MockBean
    private lateinit var amazonDynamoDb: AmazonDynamoDB

    @MockBean
    private lateinit var amazonSQS: AmazonSQS

    private val textUtils = TextUtils()

    private fun actualResult(): SourceRecord? {
        val processor = HBaseResultProcessor(textUtils)
        ReflectionTestUtils.setField(processor, "topicName", "db.a.b")
        return processor.process(result)
    }

    private fun assertResult(expected: SourceRecord, actual: SourceRecord?) {
        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals(expected.toString(), actual.toString())
    }

    private fun init(cellData: String) {
        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))

        val cell = mock<Cell> {
            on { timestamp } doReturn 100L
        }

        given(result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("record"))).willReturn(cell)
    }

    private fun expectedResult(outerType: String, innerType: String, database: String, collection: String) =
            SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject, 100L,
                database, collection, outerType, innerType)

    private fun commonBlock(database: String, collection: String): String {
        val topicBlock = """
            |    "db": "$database",
            |    "collection": "$collection"
        """.trimMargin()
        return "$topicBlock, $idBlock, $encryptionBlock"
    }

    companion object {
        const val rowId = "EXPECTED_ID"
        const val dbObject = "EXPECTED_DB_OBJECT"
        private const val encryptionKeyId = "EXPECTED_ENCRYPTION_KEY_ID"
        private const val encryptedEncryptionKey = "EXPECTED_ENCRYPTED_ENCRYPTION_KEY"
        private const val keyEncryptionKeyId = "EXPECTED_KEY_ENCRYPTION_KEY_ID"
        private const val initialisationVector = "EXPECTED_INITIALISATION_VECTOR"

        val idBlock = """
            |"_id": {
            |   "declarationId": "b0269a34-2e37-4081-b67f-ae08d0e4d813"
            |}
            |""".trimMargin()

        val encryptionBlock = """
            |"encryption": {
            |   "encryptionKeyId": "$encryptionKeyId",
            |   "encryptedEncryptionKey": "$encryptedEncryptionKey",
            |   "initialisationVector": "$initialisationVector",
            |   "keyEncryptionKeyId": "$keyEncryptionKeyId"
            |}
        """.trimMargin()

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val result: Result = Mockito.mock(Result::class.java)
    }
}

