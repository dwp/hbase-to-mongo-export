package app.services.impl

import app.services.MetricsService
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.exporter.PushGateway
import org.springframework.beans.factory.annotation.Value
import org.springframework.cache.annotation.Cacheable
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.text.SimpleDateFormat
import java.util.*

@Service
class MetricsServiceImpl(private val pushGateway: PushGateway): MetricsService {


    @Scheduled(fixedRateString = "\${metrics.push.rate:10000}", initialDelayString = "\${metrics.initial.delay:10000}")
    override fun pushMetrics() {
        pushGateway.push(CollectorRegistry.defaultRegistry, "htme", metricsGroupingKey())
    }

    @Cacheable("COUNTER_CACHE")
    @Synchronized
    override fun counter(name: String, help: String, vararg labels: String): Counter =
            with (Counter.build()) {
                name(name)
                labelNames(*labels)
                help(help)
                register()
            }

    fun metricsGroupingKey(): Map<String, String> =
        mapOf("type" to snapshotType, "topic" to topicName,
            "export_date" to SimpleDateFormat("yyyy-MM-dd").format(Date()))


    @Value("\${snapshot.type}")
    private lateinit var snapshotType: String

    @Value("\${topic.name}")
    private lateinit var topicName: String

}
