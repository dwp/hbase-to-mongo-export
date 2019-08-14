package app.configuration

import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.http.impl.client.HttpClients
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.nio.file.Files
import java.nio.file.Paths
import java.security.SecureRandom
import java.util.function.Consumer
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder


@Configuration
class ContextConfiguration {

    @Bean
    @Profile("strongRng")
    fun secureRandom() = SecureRandom.getInstanceStrong()



    @Bean
    @Profile("realHttpClient")
    fun httpClient() = HttpClients.createDefault()

    @Bean
    @Profile("realHbaseDataSource")
    fun localConnection(): Connection {

        if (dataReadyFlagLocation.isNotBlank()) {
            var attempts = 0
            val path = Paths.get(dataReadyFlagLocation)
            while (!Files.isDirectory(Paths.get(dataReadyFlagLocation)) && ++attempts < 100) {
                logger.info("Waiting for data: '$path', attempt no. $attempts.")
                Thread.sleep(3_000)
            }
        }

        val connection = ConnectionFactory.createConnection(HBaseConfiguration.create().apply {
            this.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum)
            this.setInt("hbase.zookeeper.port", 2181)

        })

        addShutdownHook(connection)
        return connection
    }

    @Bean
    @Profile("realS3DataStore")
    fun amazonS3(): AmazonS3 {

        // eu-west-1 -> EU_WEST_2 (i.e tf style to enum name)
        val updatedRegion = region.toUpperCase().replace("-", "_")
        val clientRegion = Regions.valueOf(updatedRegion)

        //This code expects that you have AWS credentials set up per:
        // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
        println("******************************Environment Vars*****************************")
        println(System.getenv("SystemRoot"))

        //val creds = BasicAWSCredentials("not_set", "not_set")
        //return  AmazonS3ClientBuilder.standard().withCredentials(AWSStaticCredentialsProvider(creds)).withRegion("us-east-1").build()
        /*return AmazonS3ClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain())
                .withRegion("us-east-1")
                .build()*/
        //amazonS3.setEndpoint("http://localhost:4572")
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://localhost:4572", "eu-west-2"))
                .withClientConfiguration(ClientConfiguration().withProtocol(Protocol.HTTP))
                .withCredentials(
                        AWSStaticCredentialsProvider(BasicAWSCredentials("not_set", "not_set")))
                .withPathStyleAccessEnabled(true)
                .disableChunkedEncoding()
                .build()
    }

    private fun addShutdownHook(connection: Connection) {
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                logger.info("Closing hbase connection: '$connection'.")
                connection.close()
            }
        })
    }

    @Value("\${aws.region}")
    private lateinit var region: String

    @Value("\${hbase.zookeeper.quorum}")
    private lateinit var hbaseZookeeperQuorum: String

    @Value("\${data.ready.flag.location:}")
    private lateinit var dataReadyFlagLocation: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ContextConfiguration::class.toString())
    }
}