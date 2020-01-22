package app.batch

import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.MissingFieldException
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.BDDMockito.given
import org.mockito.Mockito
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
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
    "aws.region=eu-west-2"
])
class HBaseReaderTest {

    private val rowId = "EXPECTED_ID"
    private val timestamp = "EXPECTED_TIMESTAMP"
    private val encryptionKeyId = "EXPECTED_ENCRYPTION_KEY_ID"
    private val encryptedEncryptionKey = "EXPECTED_ENCRYPTED_ENCRYPTION_KEY"
    private val keyEncryptionKeyId = "EXPECTED_KEY_ENCRYPTION_KEY_ID"
    private val dbObject = "EXPECTED_DB_OBJECT"
    private val initialisationVector = "EXPECTED_INITIALISATION_VECTOR"

    @Before
    fun reset() {
        Mockito.reset(connection)
    }

    @Test
    fun testRead() {
        val table: Table = Mockito.mock(Table::class.java)
        val scanner: ResultScanner = Mockito.mock(ResultScanner::class.java)
        val result: Result = Mockito.mock(Result::class.java)
        val current: Cell = Mockito.mock(Cell::class.java)

        hbaseReader.resetScanner()

        val lastModified = "2019-07-04T07:27:35.104+0000"
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
            |    },
            |    "dbObject": "$dbObject"
            |  },
            |  "version": "core-4.master.9790",
            |  "timestamp": "$timestamp"
            |}""".trimMargin()

        given(current.timestamp).willReturn(10)
        given(result.row).willReturn(rowId.toByteArray())
        given(result.current()).willReturn(current)
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        given(scanner.next()).willReturn(result)
        given(table.getScanner(ArgumentMatchers.any(Scan::class.java))).willReturn(scanner)
        given(connection.getTable(TableName.valueOf("ucfs-data"))).willReturn(table)

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), 10, expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration",lastModified)

        val actual = hbaseReader.read()

        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals("Expected the toStrings() to match as the bytearray ids make the hashcode vary when they should be the same",
            expected.toString(), actual.toString())
    }

    @Test
    fun testReadModifiedIsObject() {
        val table: Table = Mockito.mock(Table::class.java)
        val scanner: ResultScanner = Mockito.mock(ResultScanner::class.java)
        val result: Result = Mockito.mock(Result::class.java)
        val current: Cell = Mockito.mock(Cell::class.java)
        val dateKey = "\$date"
        hbaseReader.resetScanner()

        val lastModified = "2019-07-04T07:27:35.104+0000"
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

        given(current.timestamp).willReturn(10)
        given(result.row).willReturn(rowId.toByteArray())
        given(result.current()).willReturn(current)
        given(result.getValue("topic".toByteArray(), "db.a.b".toByteArray())).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        given(scanner.next()).willReturn(result)
        given(table.getScanner(ArgumentMatchers.any(Scan::class.java))).willReturn(scanner)
        given(connection.getTable(TableName.valueOf("ucfs-data"))).willReturn(table)

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), 10, expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration", lastModified)

        val actual = hbaseReader.read()

        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals("Expected the toStrings() to match as the bytearray ids make the hashcode vary when they should be the same",
                expected.toString(), actual.toString())
    }

//    @Test
    fun testReadModifiedIsAbsent() {
        val table: Table = Mockito.mock(Table::class.java)
        val scanner: ResultScanner = Mockito.mock(ResultScanner::class.java)
        val result: Result = Mockito.mock(Result::class.java)
        val current: Cell = Mockito.mock(Cell::class.java)
        hbaseReader.resetScanner()

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

        given(current.timestamp).willReturn(10)
        given(result.row).willReturn(rowId.toByteArray())
        given(result.current()).willReturn(current)
        given(result.getValue("topic".toByteArray(), "db.a.b".toByteArray())).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        given(scanner.next()).willReturn(result)
        given(table.getScanner(ArgumentMatchers.any(Scan::class.java))).willReturn(scanner)
        given(connection.getTable(TableName.valueOf("ucfs-data"))).willReturn(table)

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), 10, expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration", "1980-01-01T00:00:00.000Z")

        val actual = hbaseReader.read()

        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals("Expected the toStrings() to match as the bytearray ids make the hashcode vary when they should be the same",
                expected.toString(), actual.toString())
    }

//    @Test(expected = MissingFieldException::class)
    fun testReject() {
        val table: Table = Mockito.mock(Table::class.java)
        val scanner: ResultScanner = Mockito.mock(ResultScanner::class.java)
        val result: Result = Mockito.mock(Result::class.java)
        val current: Cell = Mockito.mock(Cell::class.java)

        hbaseReader.resetScanner()

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

        given(current.timestamp).willReturn(10)
        given(result.row).willReturn(rowId.toByteArray())
        given(result.current()).willReturn(current)
        given(result.getValue("topic".toByteArray(), "db.a.b".toByteArray())).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        given(scanner.next()).willReturn(result)
        given(table.getScanner(ArgumentMatchers.any(Scan::class.java))).willReturn(scanner)
        given(connection.getTable(TableName.valueOf("ucfs-data"))).willReturn(table)

        hbaseReader.read()
    }

    @Autowired
    private lateinit var hbaseReader: HBaseReader

    @Autowired
    private lateinit var connection: Connection

}

