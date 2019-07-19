package app.configuration

import app.services.KeyService
import org.apache.hadoop.hbase.client.Connection
import org.mockito.Mockito
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration
class TestDataSourceConfiguration {

    @Bean
    @Profile("unitTest")
    fun connection(): Connection {
        return Mockito.mock(Connection::class.java)
    }

    @Bean
    @Profile("decryptionTest")
    fun dataKeyService(): KeyService {
        return Mockito.mock(KeyService::class.java)
    }
}