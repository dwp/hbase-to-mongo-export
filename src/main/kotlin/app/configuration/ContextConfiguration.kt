package app.configuration

import app.utils.logging.logInfo
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream
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
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Paths
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

        if (dataReadyFlagLocation.isNotBlank()) {
            var attempts = 0
            val path = Paths.get(dataReadyFlagLocation)
            val maxAttempts = 100
            while (!Files.isDirectory(Paths.get(dataReadyFlagLocation)) && ++attempts < maxAttempts) {
                logInfo(logger, "Waiting for data ready flag to exist", "file_path", "$path", "attempt_no", "$attempts", "max_attempts", "$maxAttempts")
                Thread.sleep(1000)
            }
        }

        val configuration = HBaseConfiguration.create().apply {
            set("hbase.zookeeper.quorum", hbaseZookeeperQuorum)
            setInt("hbase.zookeeper.port", 2181)
            if (hbaseTimeout.toInt() > 0) {
                setInt("hbase.client.scanner.timeout.period", if (hbaseTimeout.toInt() > 0) hbaseTimeout.toInt() else 24 * 60 * 60 * 1000)
            }

        }

        val timeout = configuration.get("hbase.client.scanner.timeout.period")
        logger.info("Configuration timeout: '${timeout}'.")
        val connection = ConnectionFactory.createConnection(configuration)
        addShutdownHook(connection)
        logger.info("Got connection to hbase.")
        return connection
    }

    private fun addShutdownHook(connection: Connection) {
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                logInfo(logger, "Closing hbase connection", "connection", "$connection")
                connection.close()
            }
        })
    }

    @Value("\${hbase.scanner.timeout:-1}")
    private lateinit var hbaseTimeout: String

    @Value("\${hbase.zookeeper.quorum}")
    private lateinit var hbaseZookeeperQuorum: String

    @Value("\${data.ready.flag.location:}")
    private lateinit var dataReadyFlagLocation: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ContextConfiguration::class.toString())
    }
}
