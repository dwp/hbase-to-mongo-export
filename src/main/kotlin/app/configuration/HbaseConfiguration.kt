package app.configuration

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component

@Component
class HbaseConfiguration {

    @Bean
    fun localConnection(): Connection {

        val configuration = HBaseConfiguration.create().apply {
            set(HConstants.ZOOKEEPER_QUORUM, hbaseZookeeperQuorum)
            setInt("hbase.zookeeper.port", 2181)
            setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, hbaseTimeoutMs.toInt())
            setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, hbaseRpcTimeoutMs.toInt())
            setInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, hbaseReadRpcTimeoutMs.toInt())
            setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, hbaseClientTimeoutMs.toInt())
        }

        ContextConfiguration.logger.info("Timeout configuration",
            "scanner" to configuration.get(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD),
            "rpc" to configuration.get(HConstants.HBASE_RPC_TIMEOUT_KEY),
            "client" to configuration.get(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT))

        val connection = ConnectionFactory.createConnection(configuration)
        addShutdownHook(connection)
        ContextConfiguration.logger.info("Got connection to hbase.")
        return connection
    }

    private fun addShutdownHook(connection: Connection) {
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                ContextConfiguration.logger.info("Closing hbase connection", "connection" to "$connection")
                connection.close()
            }
        })
    }

    @Value("\${hbase.client.timeout.ms:3600000}")
    private lateinit var hbaseClientTimeoutMs: String

    @Value("\${hbase.rpc.timeout.ms:1800000}")
    private lateinit var hbaseRpcTimeoutMs: String

    @Value("\${hbase.rpc.read.timeout.ms:1800000}")
    private lateinit var hbaseReadRpcTimeoutMs: String

    @Value("\${hbase.scanner.timeout.ms:1800000}")
    private lateinit var hbaseTimeoutMs: String

    @Value("\${hbase.zookeeper.quorum}")
    private lateinit var hbaseZookeeperQuorum: String

}
