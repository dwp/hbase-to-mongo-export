package app

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.runApplication
import org.springframework.cache.annotation.EnableCaching
import org.springframework.retry.annotation.EnableRetry
import org.springframework.scheduling.annotation.EnableScheduling
import kotlin.system.exitProcess

@SpringBootApplication(exclude = [DataSourceAutoConfiguration::class])
@EnableRetry
@EnableCaching
@EnableScheduling
class HBaseToMongoExport

fun main(args: Array<String>) {
    exitProcess(SpringApplication.exit(runApplication<HBaseToMongoExport>(*args)))
}
