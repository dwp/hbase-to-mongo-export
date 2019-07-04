package uk.gov.dwp.dataworks.export.configuration

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration
@Profile("localDatasource")
class DataSourceConfiguration {
    @Bean
    fun connection(): Connection {
        val connection = ConnectionFactory.createConnection(HBaseConfiguration.create())

        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                logger.info("Closing hbase connection: '$connection'.")
                connection.close()
            }
        })

        return connection
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DataSourceConfiguration::class.toString())
    }

}