package app.configuration

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.nio.file.Files
import java.nio.file.Paths
import java.security.SecureRandom

@Configuration
class DataSourceConfiguration {

    @Bean
    @Profile("strongRng")
    fun secureRandom() = SecureRandom.getInstanceStrong()

    @Bean
    @Profile("localDataSource")
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


    private fun addShutdownHook(connection: Connection) {
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                logger.info("Closing hbase connection: '$connection'.")
                connection.close()
            }
        })
    }

    @Value("\${hbase.zookeeper.quorum:hbase}")
    private lateinit var hbaseZookeeperQuorum: String

    @Value("\${data.ready.flag.location:}")
    private lateinit var dataReadyFlagLocation: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DataSourceConfiguration::class.toString())
    }
}
