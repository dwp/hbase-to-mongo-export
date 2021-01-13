package app.configuration

import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.http.impl.client.HttpClients
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.OutputStream
import java.security.SecureRandom
import javax.crypto.Cipher


@Configuration
class ContextConfiguration {

    @Bean
    @Profile("bz2Compressor")
    fun bz2Compressor() = object: CompressionInstanceProvider {
        override fun compressorOutputStream(outputStream: OutputStream) =
                CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, outputStream)
        override fun compressionExtension() = "bz2"
    }

    @Bean
    @Profile("gzCompressor")
    fun gzCompressor() = object: CompressionInstanceProvider {
        override fun compressorOutputStream(outputStream: OutputStream) =
                CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, outputStream)
        override fun compressionExtension() = "gz"
    }

    @Bean
    @Profile("framedLZ4Compressor")
    fun framedLZ4Compressor() = object: CompressionInstanceProvider {
        override fun compressorOutputStream(outputStream: OutputStream) =
                FramedLZ4CompressorOutputStream(outputStream)

        override fun compressionExtension() = "lz4"
    }

    @Bean
    @Profile("blockLZ4Compressor")
    fun blockLZ4Compressor() = object: CompressionInstanceProvider {
        override fun compressorOutputStream(outputStream: OutputStream) =
                BlockLZ4CompressorOutputStream(outputStream)

        override fun compressionExtension() = "lz4"
    }

    @Bean
    fun cipherInstanceProvider(): CipherInstanceProvider {
        return object : CipherInstanceProvider {
            override fun cipherInstance(): Cipher {
                return Cipher.getInstance("AES/CTR/NoPadding", "BC")
            }
        }
    }

    @Bean
    @Profile("insecureHttpClient")
    fun insecureHttpClientProvider() = object : HttpClientProvider {
        override fun client() = HttpClients.createDefault()!!
    }

    @Bean
    @Profile("strongRng")
    fun secureRandom() = SecureRandom.getInstanceStrong()!!

    @Bean
    @Profile("weakRng")
    fun weakRandom() = SecureRandom.getInstance("SHA1PRNG")!!

    @Bean
    @Profile("insecureHttpClient")
    fun insecureHttpClient() = HttpClients.createDefault()!!

    @Bean
    @Profile("realHbaseDataSource")
    fun localConnection(): Connection {

        val configuration = HBaseConfiguration.create().apply {
            set(HConstants.ZOOKEEPER_QUORUM, hbaseZookeeperQuorum)
            setInt("hbase.zookeeper.port", 2181)
            setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, hbaseTimeoutMs.toInt())
            setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, hbaseRpcTimeoutMs.toInt())
            setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, hbaseClientTimeoutMs.toInt())
        }

        logger.info("Timeout configuration",
                "scanner" to configuration.get(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD),
                "rpc" to configuration.get(HConstants.HBASE_RPC_TIMEOUT_KEY),
                "client" to configuration.get(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT))

        val connection = ConnectionFactory.createConnection(configuration)
        addShutdownHook(connection)
        logger.info("Got connection to hbase.")
        return connection
    }

    private fun addShutdownHook(connection: Connection) {
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                logger.info("Closing hbase connection", "connection" to "$connection")
                connection.close()
            }
        })
    }

    @Value("\${hbase.client.timeout.ms:3600000}")
    private lateinit var hbaseClientTimeoutMs: String

    @Value("\${hbase.rpc.timeout.ms:1800000}")
    private lateinit var hbaseRpcTimeoutMs: String

    @Value("\${hbase.scanner.timeout.ms:1800000}")
    private lateinit var hbaseTimeoutMs: String

    @Value("\${hbase.zookeeper.quorum}")
    private lateinit var hbaseZookeeperQuorum: String

    companion object {
        val logger = DataworksLogger.getLogger(ContextConfiguration::class.toString())
    }
}
