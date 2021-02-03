package app.configuration

import io.micrometer.core.instrument.Metrics
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.PushGateway
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.annotation.PostConstruct

@Configuration
class MetricsConfiguration {

    val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    @Bean
    fun collectorRegistry(): CollectorRegistry = meterRegistry.prometheusRegistry

    @Bean
    fun pushGateway(): PushGateway = PushGateway(pushgatewayAddress)

    @PostConstruct
    fun init() {
        Metrics.globalRegistry.add(meterRegistry)
    }

    @Value("\${pushgateway.address}")
    private lateinit var pushgatewayAddress: String
}
