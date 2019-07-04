package uk.gov.dwp.dataworks.export

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.runApplication

@SpringBootApplication(exclude = [DataSourceAutoConfiguration::class])
class DataworksExportApplication

fun main(args: Array<String>) {
    runApplication<DataworksExportApplication>(*args)
}
