package app

import io.prometheus.client.spring.web.EnablePrometheusTiming
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.runApplication
import org.springframework.cache.annotation.EnableCaching
import org.springframework.retry.annotation.EnableRetry
import org.springframework.scheduling.annotation.EnableScheduling
import uk.gov.dwp.dataworks.logging.DataworksLogger
import uk.gov.dwp.dataworks.logging.LogFields
import kotlin.system.exitProcess

@SpringBootApplication(exclude = [DataSourceAutoConfiguration::class])
@EnableRetry
@EnableCaching
@EnableScheduling
@EnablePrometheusTiming
class HBaseToMongoExport

fun main(args: Array<String>) {
    exitProcess(SpringApplication.exit(runApplication<HBaseToMongoExport>(*args)))
}
