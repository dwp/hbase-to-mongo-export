package app.batch

import app.batch.processor.HBaseResultProcessor
import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.MissingFieldException
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.sqs.AmazonSQS
import com.nhaarman.mockitokotlin2.reset
import org.apache.hadoop.hbase.client.Result
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
import java.nio.charset.Charset

@RunWith(SpringRunner::class)
@ActiveProfiles("phoneyCipherService", "phoneyDataKeyService", "unitTest", "outputToConsole")
@SpringBootTest
@TestPropertySource(properties = [
    "data.table.name=ucfs-data",
    "column.family=topic",
    "topic.name=db.a.b",
    "identity.keystore=resources/identity.jks",
    "trust.keystore=resources/truststore.jks",
    "identity.store.password=changeit",
    "identity.key.password=changeit",
    "trust.store.password=changeit",
    "identity.store.alias=cid",
    "hbase.zookeeper.quorum=hbase",
    "aws.region=eu-west-2",
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
        assertResult(expectedResult("OUTER_TYPE", "INNER_TYPE"), actualResult())
    }

    @Test
    fun testInnerTypeWhenNoOuterType() {
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
        assertResult(expectedResult("TYPE_NOT_SET", "INNER_TYPE"), actualResult())
    }

    @Test
    fun testInnerTypeWhenEmptyOuterType() {
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
        assertResult(expectedResult("TYPE_NOT_SET", "INNER_TYPE"), actualResult())
    }

    @Test
    fun testReadModifiedIsObject() {
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
        assertResult(expectedResult("OUTER_TYPE", "INNER_TYPE"), actualResult())
    }

    @Test
    fun testReadModifiedIsAbsent() {
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
        assertResult(expectedResult("OUTER_TYPE", "INNER_TYPE"), actualResult())
    }

    @Test
    fun testReadNoType() {
        val cellData = """
            |{
            |  "message": {
            |   $commonBlock,
            |    "dbObject": "$dbObject"
            |  }
            |}""".trimMargin()

        init(cellData)
        assertResult(expectedResult("TYPE_NOT_SET","TYPE_NOT_SET"), actualResult())
    }

    @Test(expected = MissingFieldException::class)
    fun testReject() {
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

    private fun actualResult() = HBaseResultProcessor().process(result)

    private fun assertResult(expected: SourceRecord, actual: SourceRecord?) {
        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals(expected.toString(), actual.toString())
    }

    private fun init(cellData: String) {
        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
    }

    private fun expectedResult(outerType: String, innerType: String) =
            SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject,
                database, collection, outerType, innerType)

    companion object {
        const val rowId = "EXPECTED_ID"
        const val dbObject = "EXPECTED_DB_OBJECT"
        const val encryptionKeyId = "EXPECTED_ENCRYPTION_KEY_ID"
        const val encryptedEncryptionKey = "EXPECTED_ENCRYPTED_ENCRYPTION_KEY"
        const val keyEncryptionKeyId = "EXPECTED_KEY_ENCRYPTION_KEY_ID"
        const val initialisationVector = "EXPECTED_INITIALISATION_VECTOR"
        const val database = "database"
        const val collection = "collection"

        val topicBlock = """
            |    "db": "$database",
            |    "collection": "$collection"
        """.trimMargin()

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

        val commonBlock = "$topicBlock, $idBlock, $encryptionBlock"
        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val result: Result = Mockito.mock(Result::class.java)
    }

}

