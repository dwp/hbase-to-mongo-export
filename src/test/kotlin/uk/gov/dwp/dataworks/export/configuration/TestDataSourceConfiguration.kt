package uk.gov.dwp.dataworks.export.configuration

import org.apache.hadoop.hbase.client.Connection
import org.mockito.Mockito
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration
@Profile("unitTest")
class TestDataSourceConfiguration {

    @Bean
    fun connection(): Connection {
        return Mockito.mock(Connection::class.java)
    }
}