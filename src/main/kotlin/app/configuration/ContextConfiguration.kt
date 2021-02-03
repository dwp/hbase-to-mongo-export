package app.configuration

import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream
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
    @Profile("!weakRng")
    fun secureRandom() = SecureRandom.getInstanceStrong()!!

    @Bean
    @Profile("weakRng")
    fun weakRandom() = SecureRandom.getInstance("SHA1PRNG")!!

    companion object {
        val logger = DataworksLogger.getLogger(ContextConfiguration::class)
    }
}
