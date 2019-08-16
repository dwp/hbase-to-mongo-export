package app.batch

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.PutObjectRequest
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@ActiveProfiles("phoneyDataKeyService", "phoneyCipherService", "unitTest", "outputToS3")
@SpringBootTest
@TestPropertySource(properties = [
    "directory.output=ephemera",
    "output.batch.size.max.bytes=100000",
    "source.table.name=ucdata",
    "compress.output=true",
    "encrypt.output=true",
    "data.table.name=ucfs-data",
    "column.family=topic",
    "topic.name=db.a.b",
    "aws.region=eu-west-1",
    "s3.bucket=not_set",
    "s3.prefix.folder=not_set",
    "identity.keystore=resources/identity.jks",
    "trust.keystore=resources/truststore.jks",
    "identity.store.password=changeit",
    "identity.key.password=changeit",
    "trust.store.password=changeit",
    "identity.store.alias=cid",
    "hbase.zookeeper.quorum=hbase"
])

class S3DirectoryWriterTest {

    @Test
    fun testWriteData() {

        val listOfLists: MutableList<MutableList<String>> = mutableListOf()
        var total = 0

        for (i in 1..10) {
            val list: MutableList<String> = mutableListOf()
            for (j in 1..10) {
                val token = "[%03d/%04d]".format(i, j)
                val item = token.repeat(j * (11 - i) * 10)
                list.add(item)
                total += item.length
            }
            listOfLists.add(list)
        }
        listOfLists.forEach {
            s3DirectoryWriter.write(it)
        }

        s3DirectoryWriter.writeOutput()
        Mockito.verify(s3Client, Mockito.times(4))
                .putObject(ArgumentMatchers.any(PutObjectRequest::class.java))
    }

    @Autowired
    private lateinit var s3DirectoryWriter: S3DirectoryWriter

    @Autowired
    private lateinit var s3Client: AmazonS3

    companion object {
        val logger: Logger = LoggerFactory.getLogger(S3DirectoryWriterTest::class.toString())
    }

}
