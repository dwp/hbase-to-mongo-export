package app.services.impl

import app.services.PushGatewayService
import app.utils.PropertyUtility
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.PushGateway
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor
import org.springframework.scheduling.config.ScheduledTask
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.text.SimpleDateFormat
import java.util.*
@Service
class PushGatewayServiceImpl(private val pushGateway: PushGateway,
                             private val postProcessor: ScheduledAnnotationBeanPostProcessor): PushGatewayService {

    @Scheduled(fixedRateString = "\${metrics.push.rate:20000}", initialDelayString = "\${metrics.initial.delay:10000}")
    override fun pushMetrics() {
        pushGateway.push(CollectorRegistry.defaultRegistry, "htme", metricsGroupingKey())
        logger.info("Pushed metrics", *metricsGroupingKeyPairs())
    }

    override fun pushFinalMetrics() {
        logger.info("Pushing final set of metrics", *metricsGroupingKeyPairs())
        postProcessor.scheduledTasks.forEach(ScheduledTask::cancel)
        pushMetrics()
        deleteMetrics()
        logger.info("Pushed final set of metrics", *metricsGroupingKeyPairs())
    }

    override fun deleteMetrics() {
        if (deleteMetrics.toBoolean()) {
            logger.info("Waiting for metric collection before deletion", "scrape_interval" to scrapeInterval,
                *metricsGroupingKeyPairs())
            Thread.sleep(scrapeInterval.toLong())
            pushGateway.delete("htme", metricsGroupingKey())
            logger.info("Deleted metrics", "scrape_interval" to scrapeInterval, *metricsGroupingKeyPairs())
        }
    }

    private fun metricsGroupingKeyPairs(): Array<Pair<String, String>> =
        metricsGroupingKey().entries.map { (k, v) -> Pair(k, v) }.toTypedArray()

    private fun metricsGroupingKey(): Map<String, String> =
        mapOf("type" to snapshotType, "topic" to topicName, "instance" to instanceName,
            "correlation_id" to PropertyUtility.correlationId(),
            "export_date" to SimpleDateFormat("yyyy-MM-dd").format(Date()))


    @Value("\${snapshot.type}")
    private lateinit var snapshotType: String

    @Value("\${topic.name}")
    private lateinit var topicName: String

    @Value("\${instance.name}")
    private lateinit var instanceName: String

    @Value("\${prometheus.scrape.interval:70000}")
    private lateinit var scrapeInterval: String

    @Value("\${delete.metrics:true}")
    private lateinit var deleteMetrics: String

    companion object {
        private val logger = DataworksLogger.getLogger(PushGatewayServiceImpl::class)
    }
}

