package app.services

import io.prometheus.client.Counter

interface MetricsService {
    fun pushMetrics()
    fun counter(name: String, help: String, vararg labels: String): Counter
}
