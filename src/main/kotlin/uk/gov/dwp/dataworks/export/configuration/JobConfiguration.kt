package uk.gov.dwp.dataworks.export.configuration

import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.support.CompositeItemProcessor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.transaction.PlatformTransactionManager
import uk.gov.dwp.dataworks.export.domain.SourceRecord
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
@Profile("batchRun")
class JobConfiguration : DefaultBatchConfigurer() {

    @Bean(name = ["nightlyExportBatchJob"])
    fun job(step: Step) = jobBuilderFactory.get("exportJob").start(step).build()

    @Bean
    fun step() = stepBuilderFactory.get("step")
            .chunk<SourceRecord, String>(10)
            .reader(itemReader)
            .faultTolerant()
            .skip(IllegalStateException::class.java)
            .skipLimit(Integer.MAX_VALUE)
            .processor(processor())
            .writer(itemWriter)
            .build()

    fun processor(): ItemProcessor<SourceRecord, String> {
        val compositeProcessor: CompositeItemProcessor<SourceRecord, String> = CompositeItemProcessor()
        val delegates = listOf(decryptionProcessor, jsonProcessor)
        compositeProcessor.setDelegates(delegates)
        return compositeProcessor
    }

    @Bean(name = ["mapJobRepositoryFactoryBean"])
    fun createMapJobRepositoryFactory(transactionManager: PlatformTransactionManager): MapJobRepositoryFactoryBean {
        val mapJobRepositoryFactoryBean = MapJobRepositoryFactoryBean()
        mapJobRepositoryFactoryBean.transactionManager = transactionManager
        return mapJobRepositoryFactoryBean
    }

    override fun setDataSource(dataSource: DataSource) {}

    @Autowired
    lateinit var itemReader: ItemReader<SourceRecord>

    @Autowired
    lateinit var decryptionProcessor: ItemProcessor<SourceRecord, SourceRecord>

    @Autowired
    lateinit var jsonProcessor: ItemProcessor<SourceRecord, String>

    @Autowired
    lateinit var itemWriter: ItemWriter<String>

    @Autowired
    lateinit var jobBuilderFactory: JobBuilderFactory

    @Autowired
    lateinit var stepBuilderFactory: StepBuilderFactory

}