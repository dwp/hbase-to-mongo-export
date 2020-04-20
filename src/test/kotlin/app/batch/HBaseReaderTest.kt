package app.batch

import app.domain.EncryptionBlock
import app.domain.SourceRecord
import app.exceptions.MissingFieldException
import app.utils.TextUtils
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.BDDMockito.given
import org.mockito.Mockito
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


    @Test
    fun testRead() {
        val table: Table = Mockito.mock(Table::class.java)
        val scanner: ResultScanner = Mockito.mock(ResultScanner::class.java)
        val result: Result = Mockito.mock(Result::class.java)
        val current: Cell = Mockito.mock(Cell::class.java)
        val connection: Connection = Mockito.mock(Connection::class.java)
        val textUtils: TextUtils = Mockito.mock(TextUtils::class.java)
        val matchResult: MatchResult = Mockito.mock(MatchResult::class.java)
        val matches = listOf("db", "database", "collection")
        given(matchResult.groupValues).willReturn(matches)
        given(textUtils.topicNameTableMatcher(ArgumentMatchers.anyString())).willReturn(matchResult)

        val hbaseReader = HBaseReader(connection, textUtils)
        hbaseReader.resetScanner()

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
        given(connection.getTable(ArgumentMatchers.any(TableName::class.java))).willReturn(table)

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), 10, expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration",lastModified, "OUTER_TYPE", "INNER_TYPE")

        val actual = hbaseReader.read()

        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals("Expected the toStrings() to match as the bytearray ids make the hashcode vary when they should be the same",
            expected.toString(), actual.toString())
    }

    @Test
    fun testInnerTypeWhenNoOuterType() {
        val table: Table = Mockito.mock(Table::class.java)
        val scanner: ResultScanner = Mockito.mock(ResultScanner::class.java)
        val result: Result = Mockito.mock(Result::class.java)
        val current: Cell = Mockito.mock(Cell::class.java)
        val connection: Connection = Mockito.mock(Connection::class.java)
        val textUtils: TextUtils = Mockito.mock(TextUtils::class.java)
        val matchResult: MatchResult = Mockito.mock(MatchResult::class.java)
        val matches = listOf("db", "database", "collection")
        given(matchResult.groupValues).willReturn(matches)
        given(textUtils.topicNameTableMatcher(ArgumentMatchers.anyString())).willReturn(matchResult)

        val hbaseReader = HBaseReader(connection, textUtils)
        hbaseReader.resetScanner()

        val lastModified = "2019-07-04T07:27:35.104+0000"
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

        given(current.timestamp).willReturn(10)
        given(result.row).willReturn(rowId.toByteArray())
        given(result.current()).willReturn(current)
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        given(scanner.next()).willReturn(result)
        given(table.getScanner(ArgumentMatchers.any(Scan::class.java))).willReturn(scanner)
        given(connection.getTable(ArgumentMatchers.any(TableName::class.java))).willReturn(table)

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), 10, expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration",lastModified, "TYPE_NOT_SET", "INNER_TYPE")

        val actual = hbaseReader.read()

        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals("Expected the toStrings() to match as the bytearray ids make the hashcode vary when they should be the same",
                expected.toString(), actual.toString())
    }

    @Test
    fun testInnerTypeWhenEmptyOuterType() {
        val table: Table = Mockito.mock(Table::class.java)
        val scanner: ResultScanner = Mockito.mock(ResultScanner::class.java)
        val result: Result = Mockito.mock(Result::class.java)
        val current: Cell = Mockito.mock(Cell::class.java)
        val connection: Connection = Mockito.mock(Connection::class.java)
        val textUtils: TextUtils = Mockito.mock(TextUtils::class.java)
        val matchResult: MatchResult = Mockito.mock(MatchResult::class.java)
        val matches = listOf("db", "database", "collection")
        given(matchResult.groupValues).willReturn(matches)
        given(textUtils.topicNameTableMatcher(ArgumentMatchers.anyString())).willReturn(matchResult)

        val hbaseReader = HBaseReader(connection, textUtils)
        hbaseReader.resetScanner()

        val lastModified = "2019-07-04T07:27:35.104+0000"
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

        given(current.timestamp).willReturn(10)
        given(result.row).willReturn(rowId.toByteArray())
        given(result.current()).willReturn(current)
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        given(scanner.next()).willReturn(result)
        given(table.getScanner(ArgumentMatchers.any(Scan::class.java))).willReturn(scanner)
        given(connection.getTable(ArgumentMatchers.any(TableName::class.java))).willReturn(table)

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), 10, expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration",lastModified, "TYPE_NOT_SET", "INNER_TYPE")

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
        val connection: Connection = Mockito.mock(Connection::class.java)
        val textUtils: TextUtils = Mockito.mock(TextUtils::class.java)
        val matchResult: MatchResult = Mockito.mock(MatchResult::class.java)
        val matches = listOf("db", "database", "collection")
        given(matchResult.groupValues).willReturn(matches)
        given(textUtils.topicNameTableMatcher(ArgumentMatchers.anyString())).willReturn(matchResult)

        val hbaseReader = HBaseReader(connection, textUtils)
        hbaseReader.resetScanner()

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

        given(current.timestamp).willReturn(10)
        given(result.row).willReturn(rowId.toByteArray())
        given(result.current()).willReturn(current)
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        given(scanner.next()).willReturn(result)
        given(table.getScanner(ArgumentMatchers.any(Scan::class.java))).willReturn(scanner)
        given(connection.getTable(ArgumentMatchers.any(TableName::class.java))).willReturn(table)

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), 10, expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration", lastModified, "OUTER_TYPE", "INNER_TYPE")

        val actual = hbaseReader.read()

        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals("Expected the toStrings() to match as the bytearray ids make the hashcode vary when they should be the same",
                expected.toString(), actual.toString())
    }

    @Test
    fun testReadModifiedIsAbsent() {
        val table: Table = Mockito.mock(Table::class.java)
        val scanner: ResultScanner = Mockito.mock(ResultScanner::class.java)
        val result: Result = Mockito.mock(Result::class.java)
        val current: Cell = Mockito.mock(Cell::class.java)
        val connection: Connection = Mockito.mock(Connection::class.java)
        val textUtils: TextUtils = Mockito.mock(TextUtils::class.java)
        val matchResult: MatchResult = Mockito.mock(MatchResult::class.java)
        val matches = listOf("db", "database", "collection")
        given(matchResult.groupValues).willReturn(matches)
        given(textUtils.topicNameTableMatcher(ArgumentMatchers.anyString())).willReturn(matchResult)

        val hbaseReader = HBaseReader(connection, textUtils)

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

        given(current.timestamp).willReturn(10)
        given(result.row).willReturn(rowId.toByteArray())
        given(result.current()).willReturn(current)
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        given(scanner.next()).willReturn(result)
        given(table.getScanner(ArgumentMatchers.any(Scan::class.java))).willReturn(scanner)
        given(connection.getTable(ArgumentMatchers.any(TableName::class.java))).willReturn(table)

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), 10, expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration", "1980-01-01T00:00:00.000Z", "OUTER_TYPE", "INNER_TYPE")

        val actual = hbaseReader.read()

        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals("Expected the toStrings() to match as the bytearray ids make the hashcode vary when they should be the same",
                expected.toString(), actual.toString())
    }

    @Test
    fun testReadNoType() {
        val table: Table = Mockito.mock(Table::class.java)
        val scanner: ResultScanner = Mockito.mock(ResultScanner::class.java)
        val result: Result = Mockito.mock(Result::class.java)
        val current: Cell = Mockito.mock(Cell::class.java)
        val connection: Connection = Mockito.mock(Connection::class.java)
        val textUtils: TextUtils = Mockito.mock(TextUtils::class.java)
        val matchResult: MatchResult = Mockito.mock(MatchResult::class.java)
        val matches = listOf("db", "database", "collection")
        given(matchResult.groupValues).willReturn(matches)
        given(textUtils.topicNameTableMatcher(ArgumentMatchers.anyString())).willReturn(matchResult)

        val hbaseReader = HBaseReader(connection, textUtils)

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

        given(current.timestamp).willReturn(10)
        given(result.row).willReturn(rowId.toByteArray())
        given(result.current()).willReturn(current)
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        given(scanner.next()).willReturn(result)
        given(table.getScanner(ArgumentMatchers.any(Scan::class.java))).willReturn(scanner)
        given(connection.getTable(ArgumentMatchers.any(TableName::class.java))).willReturn(table)

        val expectedEncryptionBlock = EncryptionBlock(keyEncryptionKeyId, initialisationVector, encryptedEncryptionKey)
        val expected = SourceRecord(rowId.toByteArray(), 10, expectedEncryptionBlock, dbObject,
                "core", "addressDeclaration", "1980-01-01T00:00:00.000Z",
                "TYPE_NOT_SET",
                "TYPE_NOT_SET")

        val actual = hbaseReader.read()

        assertEquals(expected.dbObject, actual?.dbObject)
        assertEquals("Expected the toStrings() to match as the bytearray ids make the hashcode vary when they should be the same",
                expected.toString(), actual.toString())
    }

    @Test(expected = MissingFieldException::class)
    fun testReject() {
        val table: Table = Mockito.mock(Table::class.java)
        val scanner: ResultScanner = Mockito.mock(ResultScanner::class.java)
        val result: Result = Mockito.mock(Result::class.java)
        val current: Cell = Mockito.mock(Cell::class.java)

        val connection: Connection = Mockito.mock(Connection::class.java)
        val textUtils: TextUtils = Mockito.mock(TextUtils::class.java)
        val matchResult: MatchResult = Mockito.mock(MatchResult::class.java)
        val matches = listOf("db", "database", "collection")
        given(matchResult.groupValues).willReturn(matches)
        given(textUtils.topicNameTableMatcher(ArgumentMatchers.anyString())).willReturn(matchResult)

        val hbaseReader = HBaseReader(connection, textUtils)

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
        given(result.value()).willReturn(cellData.toByteArray(Charset.defaultCharset()))
        given(scanner.next()).willReturn(result)
        given(table.getScanner(ArgumentMatchers.any(Scan::class.java))).willReturn(scanner)
        given(connection.getTable(ArgumentMatchers.any(TableName::class.java))).willReturn(table)
        hbaseReader.read()
    }
}

