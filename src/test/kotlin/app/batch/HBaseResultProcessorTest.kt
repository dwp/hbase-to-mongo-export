package app.batch

import app.batch.processor.HBaseResultProcessor
import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.MissingFieldException
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.sqs.AmazonSQS
import org.apache.hadoop.hbase.client.Result
import org.junit.Assert.assertEquals
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
    "trigger.snapshot.sender=false"
])
class HBaseResultProcessorTest {


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

    }

    @Test
    fun testRead() {
        val result: Result = Mockito.mock(Result::class.java)
        val hbaseResultProcessor = HBaseResultProcessor()

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
        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject,
                database, collection,"OUTER_TYPE", "INNER_TYPE")

        val actual = hbaseResultProcessor.process(result)

        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals(expected.toString(), actual.toString())
    }

    @Test
    fun testInnerTypeWhenNoOuterType() {
        val result: Result = Mockito.mock(Result::class.java)
        val hbaseResultProcessor = HBaseResultProcessor()
        val cellData = """
            |{
            |  "message": {
            |   $commonBlock,
            |    "@type": "INNER_TYPE",
            |    "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |    "dbObject": "$dbObject"
            |  }
            |}""".trimMargin()

        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject,
                database, collection, "TYPE_NOT_SET", "INNER_TYPE")

        val actual = hbaseResultProcessor.process(result)

        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals(expected.toString(), actual.toString())
    }

    @Test
    fun testInnerTypeWhenEmptyOuterType() {
        val result: Result = Mockito.mock(Result::class.java)
        val hbaseResultProcessor = HBaseResultProcessor()

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

        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject,
                database, collection, "TYPE_NOT_SET", "INNER_TYPE")
        val actual = hbaseResultProcessor.process(result)
        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals(expected.toString(), actual.toString())
    }

    @Test
    fun testReadModifiedIsObject() {
        val result: Result = Mockito.mock(Result::class.java)
        val hbaseResultProcessor = HBaseResultProcessor()
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

        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject,
                database, collection, "OUTER_TYPE", "INNER_TYPE")

        val actual = hbaseResultProcessor.process(result)

        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals(expected.toString(), actual.toString())
    }

    @Test
    fun testReadModifiedIsAbsent() {
        val result: Result = Mockito.mock(Result::class.java)
        val hbaseResultProcessor = HBaseResultProcessor()

        val cellData = """
            |{
            |  "@type": "OUTER_TYPE",
            |  "message": {
            |   $commonBlock,
            |    "@type": "INNER_TYPE",
            |    "dbObject": "$dbObject"
            |  }
            |}""".trimMargin()

        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject,
                database, collection, "OUTER_TYPE", "INNER_TYPE")
        val actual = hbaseResultProcessor.process(result)
        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals(expected.toString(), actual.toString())
    }

    @Test
    fun testReadNoType() {
        val result: Result = Mockito.mock(Result::class.java)
        val hbaseResultProcessor = HBaseResultProcessor()

        val cellData = """
            |{
            |  "message": {
            |   $commonBlock,
            |    "dbObject": "$dbObject"
            |  }
            |}""".trimMargin()

        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject,
                database, collection,
                "TYPE_NOT_SET",
                "TYPE_NOT_SET")
        val actual = hbaseResultProcessor.process(result)
        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals(expected.toString(), actual.toString())
    }

    @Test(expected = MissingFieldException::class)
    fun testReject() {
        val result: Result = Mockito.mock(Result::class.java)
        val hbaseResultProcessor = HBaseResultProcessor()

        val cellData = """
            |{
            |  "@type": "V4",
            |  "message": {
            |   $commonBlock,
            |    "@type": "MONGO_INSERT",
            |    "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000"
            |  }
            |}""".trimMargin()

        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        hbaseResultProcessor.process(result)
    }

    @MockBean
    private lateinit var amazonDynamoDb: AmazonDynamoDB

    @MockBean
    private lateinit var amazonSQS: AmazonSQS
}

