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

    private val rowId = "EXPECTED_ID"
    private val timestamp = "EXPECTED_TIMESTAMP"
    private val encryptionKeyId = "EXPECTED_ENCRYPTION_KEY_ID"
    private val encryptedEncryptionKey = "EXPECTED_ENCRYPTED_ENCRYPTION_KEY"
    private val keyEncryptionKeyId = "EXPECTED_KEY_ENCRYPTION_KEY_ID"
    private val dbObject = "EXPECTED_DB_OBJECT"
    private val initialisationVector = "EXPECTED_INITIALISATION_VECTOR"


    @Test
    fun testRead() {
        val result: Result = Mockito.mock(Result::class.java)
        val hbaseResultProcessor = HBaseResultProcessor()
        val cellData = """
            |{
            |  "traceId": "3b195725-98e1-4d56-bcb8-945a244c2d45",
            |  "unitOfWorkId": "ed9e614c-cd28-4860-b77d-ab5962a5599e",
            |  "@type": "OUTER_TYPE",
            |  "message": {
            |    "db": "core",
            |    "collection": "addressDeclaration",
            |    "_id": {
            |      "declarationId": "b0269a34-2e37-4081-b67f-ae08d0e4d813"
            |    },
            |    "_timeBasedHash": "hashhhhhhhhhh",
            |    "@type": "INNER_TYPE",
            |    "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |    "encryption": {
            |      "encryptionKeyId": "$encryptionKeyId",
            |      "encryptedEncryptionKey": "$encryptedEncryptionKey",
            |      "initialisationVector": "$initialisationVector",
            |      "keyEncryptionKeyId": "$keyEncryptionKeyId"
            |    },
            |    "dbObject": "$dbObject"
            |  },
            |  "version": "core-4.master.9790",
            |  "timestamp": "$timestamp"
            |}""".trimMargin()

        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration","OUTER_TYPE", "INNER_TYPE")

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
            |  "traceId": "3b195725-98e1-4d56-bcb8-945a244c2d45",
            |  "unitOfWorkId": "ed9e614c-cd28-4860-b77d-ab5962a5599e",
            |  "message": {
            |    "db": "core",
            |    "collection": "addressDeclaration",
            |    "_id": {
            |      "declarationId": "b0269a34-2e37-4081-b67f-ae08d0e4d813"
            |    },
            |    "_timeBasedHash": "hashhhhhhhhhh",
            |    "@type": "INNER_TYPE",
            |    "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |    "encryption": {
            |      "encryptionKeyId": "$encryptionKeyId",
            |      "encryptedEncryptionKey": "$encryptedEncryptionKey",
            |      "initialisationVector": "$initialisationVector",
            |      "keyEncryptionKeyId": "$keyEncryptionKeyId"
            |    },
            |    "dbObject": "$dbObject"
            |  },
            |  "version": "core-4.master.9790",
            |  "timestamp": "$timestamp"
            |}""".trimMargin()

        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration", "TYPE_NOT_SET", "INNER_TYPE")

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
            |  "traceId": "3b195725-98e1-4d56-bcb8-945a244c2d45",
            |  "unitOfWorkId": "ed9e614c-cd28-4860-b77d-ab5962a5599e",
            |  "@type": "",
            |  "message": {
            |    "db": "core",
            |    "collection": "addressDeclaration",
            |    "_id": {
            |      "declarationId": "b0269a34-2e37-4081-b67f-ae08d0e4d813"
            |    },
            |    "_timeBasedHash": "hashhhhhhhhhh",
            |    "@type": "INNER_TYPE",
            |    "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |    "encryption": {
            |      "encryptionKeyId": "$encryptionKeyId",
            |      "encryptedEncryptionKey": "$encryptedEncryptionKey",
            |      "initialisationVector": "$initialisationVector",
            |      "keyEncryptionKeyId": "$keyEncryptionKeyId"
            |    },
            |    "dbObject": "$dbObject"
            |  },
            |  "version": "core-4.master.9790",
            |  "timestamp": "$timestamp"
            |}""".trimMargin()

        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration", "TYPE_NOT_SET", "INNER_TYPE")
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
            |  "traceId": "3b195725-98e1-4d56-bcb8-945a244c2d45",
            |  "unitOfWorkId": "ed9e614c-cd28-4860-b77d-ab5962a5599e",
            |  "@type": "OUTER_TYPE",
            |  "message": {
            |    "db": "core",
            |    "collection": "addressDeclaration",
            |    "_id": {
            |      "declarationId": "b0269a34-2e37-4081-b67f-ae08d0e4d813"
            |    },
            |    "_timeBasedHash": "hashhhhhhhhhh",
            |    "@type": "INNER_TYPE",
            |    "_lastModifiedDateTime": {
            |       $dateKey: "$lastModified"
            |    },
            |    "encryption": {
            |      "encryptionKeyId": "$encryptionKeyId",
            |      "encryptedEncryptionKey": "$encryptedEncryptionKey",
            |      "initialisationVector": "$initialisationVector",
            |      "keyEncryptionKeyId": "$keyEncryptionKeyId"
            |    },
            |    "dbObject": "$dbObject"
            |  },
            |  "version": "core-4.master.9790",
            |  "timestamp": "$timestamp"
            |}""".trimMargin()

        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration", "OUTER_TYPE", "INNER_TYPE")

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
            |  "traceId": "3b195725-98e1-4d56-bcb8-945a244c2d45",
            |  "unitOfWorkId": "ed9e614c-cd28-4860-b77d-ab5962a5599e",
            |  "@type": "OUTER_TYPE",
            |  "message": {
            |    "db": "core",
            |    "collection": "addressDeclaration",
            |    "_id": {
            |      "declarationId": "b0269a34-2e37-4081-b67f-ae08d0e4d813"
            |    },
            |    "_timeBasedHash": "hashhhhhhhhhh",
            |    "@type": "INNER_TYPE",
            |    "encryption": {
            |      "encryptionKeyId": "$encryptionKeyId",
            |      "encryptedEncryptionKey": "$encryptedEncryptionKey",
            |      "initialisationVector": "$initialisationVector",
            |      "keyEncryptionKeyId": "$keyEncryptionKeyId"
            |    },
            |    "dbObject": "$dbObject"
            |  },
            |  "version": "core-4.master.9790",
            |  "timestamp": "$timestamp"
            |}""".trimMargin()

        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration", "OUTER_TYPE", "INNER_TYPE")
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
            |  "traceId": "3b195725-98e1-4d56-bcb8-945a244c2d45",
            |  "unitOfWorkId": "ed9e614c-cd28-4860-b77d-ab5962a5599e",
            |  "message": {
            |    "db": "core",
            |    "collection": "addressDeclaration",
            |    "_id": {
            |      "declarationId": "b0269a34-2e37-4081-b67f-ae08d0e4d813"
            |    },
            |    "_timeBasedHash": "hashhhhhhhhhh",
            |    "encryption": {
            |      "encryptionKeyId": "$encryptionKeyId",
            |      "encryptedEncryptionKey": "$encryptedEncryptionKey",
            |      "initialisationVector": "$initialisationVector",
            |      "keyEncryptionKeyId": "$keyEncryptionKeyId"
            |    },
            |    "dbObject": "$dbObject"
            |  },
            |  "version": "core-4.master.9790",
            |  "timestamp": "$timestamp"
            |}""".trimMargin()

        given(result.row).willReturn(rowId.toByteArray())
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration",
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
            |  "traceId": "3b195725-98e1-4d56-bcb8-945a244c2d45",
            |  "unitOfWorkId": "ed9e614c-cd28-4860-b77d-ab5962a5599e",
            |  "@type": "V4",
            |  "message": {
            |    "db": "core",
            |    "collection": "addressDeclaration",
            |    "_id": {
            |      "declarationId": "b0269a34-2e37-4081-b67f-ae08d0e4d813"
            |    },
            |    "_timeBasedHash": "hashhhhhhhhhh",
            |    "@type": "MONGO_INSERT",
            |    "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |    "encryption": {
            |      "encryptionKeyId": "$encryptionKeyId",
            |      "encryptedEncryptionKey": "$encryptedEncryptionKey",
            |      "initialisationVector": "$initialisationVector",
            |      "keyEncryptionKeyId": "$keyEncryptionKeyId"
            |    }
            |  },
            |  "version": "core-4.master.9790",
            |  "timestamp": "$timestamp"
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

