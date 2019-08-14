package app.configuration

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.SSLContexts
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.security.SecureRandom
import javax.net.ssl.SSLContext

@Configuration
class ContextConfiguration {

    @Bean
    @Profile("secureHttpClient")
    fun secureHttpClient() = HttpClients.custom()
            .setSSLSocketFactory(connectionFactory())
            .build()!!

    fun connectionFactory() = SSLConnectionSocketFactory(
                sslContext(),
                arrayOf("TLSv1"),
                null,
                SSLConnectionSocketFactory.getDefaultHostnameVerifier())

    fun sslContext(): SSLContext =
        SSLContexts.custom()
                .loadKeyMaterial(
                        File(identityStore),
                        identityStorePassword.toCharArray(),
                        identityKeyPassword.toCharArray()) {_, socket -> identityStoreAlias}
                .loadTrustMaterial(File(trustStore), trustStorePassword.toCharArray())
                .build()

    @Bean
    @Profile("strongRng")
    fun secureRandom() = SecureRandom.getInstanceStrong()!!

    @Bean
    @Profile("insecureHttpClient")
    fun insecureHttpClient() = HttpClients.createDefault()!!

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
    @Profile("S3Client")
    fun amazonS3(): AmazonS3 {

        // eu-west-1 -> EU_WEST_2 (i.e tf style to enum name)
        val updatedRegion = region.toUpperCase().replace("-", "_")
        val clientRegion = Regions.valueOf(updatedRegion)

        //This code expects that you have AWS credentials set up per:
        // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
        return AmazonS3ClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain())
                .withRegion(clientRegion)
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
    @Value("\${identity.keystore}")
    private lateinit var identityStore: String

    @Value("\${identity.store.password}")
    private lateinit var identityStorePassword: String

    @Value("\${identity.store.alias}")
    private lateinit var identityStoreAlias: String

    @Value("\${identity.key.password}")
    private lateinit var identityKeyPassword: String

    @Value("\${trust.keystore}")
    private lateinit var trustStore: String

    @Value("\${trust.store.password}")
    private lateinit var trustStorePassword: String
  
    @Value("\${hbase.zookeeper.quorum}")
    private lateinit var hbaseZookeeperQuorum: String

    @Value("\${data.ready.flag.location:}")
    private lateinit var dataReadyFlagLocation: String

    @Value("\${aws.region}")
    private lateinit var region: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ContextConfiguration::class.toString())
    }
}
