package app.configuration

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Metrics
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.exporter.PushGateway
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.annotation.PostConstruct
import io.prometheus.client.Gauge

@Configuration
class MetricsConfiguration {

    @Bean
    fun recordCounter(): Counter =
        counter("htme_records_written", "Total number of records written to s3", "split")

    @Bean
    fun byteCounter(): Counter =
        counter("htme_bytes_written", "Total number of bytes written to s3", "split")

    @Bean
    fun successfulCollectionCounter(): Counter =
        counter("htme_successful_collections", "Number of collections completed successfully")

    @Bean
    fun emptyCollectionCounter(): Counter =
        counter("htme_successful_empty_collections", "Number of collections with no files exported completed successfully")

    @Bean
    fun successfulNonEmptyCollectionCounter(): Counter =
        counter("htme_successful_non_empty_collections", "Number of exported collections with files exported completed successfully")

    @Bean
    fun failedCollectionCounter(): Counter =
        counter("htme_failed_collections", "Number of no files exported collections completed successfully")

    @Bean
    fun scanRetriesCounter(): Counter =
        counter("htme_retried_scans", "How often a failed scan had to be re-opened.", "split")

    @Bean
    fun failedScansCounter(): Counter =
        counter("htme_failed_scans", "How often a scan failed and retries were exhausted.", "split")

    @Bean
    fun failedBatchPutCounter(): Counter =
        counter("htme_failed_batch_puts", "The no. of failed attempts to put batches into s3.", "split")

    @Bean
    fun retriedBatchPutCounter(): Counter =
        counter("htme_retried_batch_puts", "The no. of retried attempts to put batches into s3.")

    @Bean
    fun failedManifestPutCounter(): Counter =
        counter("htme_failed_manifest_puts", "The no. of failed attempts to put manifests into s3.", "split")

    @Bean
    fun retriedManifestPutCounter(): Counter =
        counter("htme_retried_manifest_puts", "The no. of retried attempts to put manifests into s3.")

    @Bean
    fun failedValidationCounter(): Counter =
        counter("htme_records_failed_validation", "The no. of records that failed validation.")

    @Bean
    fun dksDecryptKeyRetriesCounter(): Counter =
        counter("htme_dks_decrypt_retries", "The no. of dks decrypt key retries.")

    @Bean
    fun dksNewDataKeyRetriesCounter(): Counter =
        counter("htme_dks_new_key_retries", "The no. of dks new datakey request retries.")

    @Bean
    fun runningApplicationsGauge(): Gauge =
        gauge("htme_running_applications", "Number of running applications.")

    @Bean
    fun pushGateway(): PushGateway = PushGateway("$pushgatewayHost:$pushgatewayPort")


    @PostConstruct
    fun init() {
        Metrics.globalRegistry.add(PrometheusMeterRegistry(PrometheusConfig.DEFAULT, CollectorRegistry.defaultRegistry, Clock.SYSTEM))
    }

    @Synchronized
    private fun counter(name: String, help: String, vararg labels: String): Counter =
            with (Counter.build()) {
                name(name)
                labelNames(*labels)
                help(help)
                register()
            }

    @Synchronized
    private fun gauge(name: String, help: String, vararg labels: String): Gauge =
            with (Gauge.build()) {
                name(name)
                labelNames(*labels)
                help(help)
                register()
            }

    @Value("\${pushgateway.host}")
    private lateinit var pushgatewayHost: String

    @Value("\${pushgateway.port:9091}")
    private lateinit var pushgatewayPort: String
}
