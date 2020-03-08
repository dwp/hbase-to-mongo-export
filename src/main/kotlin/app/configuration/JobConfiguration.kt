package app.configuration

import app.domain.DecryptedRecord
import app.domain.Record
import app.domain.SourceRecord
import app.exceptions.BadDecryptedDataException
import app.exceptions.DecryptionFailureException
import app.exceptions.MissingFieldException
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException
import org.springframework.batch.core.configuration.annotation.*
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.partition.support.Partitioner
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.support.CompositeItemProcessor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.core.task.SimpleAsyncTaskExecutor


@Configuration
@Profile("batchRun")
@EnableBatchProcessing
class JobConfiguration : DefaultBatchConfigurer() {

    @Bean
    fun importUserJob() =
            jobBuilderFactory.get("nightlyExportBatchJob")
                    .incrementer(RunIdIncrementer())
                    .flow(step1())
                    .end()
                    .build()

    // Master
    @Bean
    fun step1() = stepBuilderFactory["step1"]
                .partitioner(slaveStep().name, partitioner)
                .step(slaveStep())
                .gridSize(256 / scanWidth.toInt())
                .taskExecutor(taskExecutor())
                .build()

    // slave step
    @Bean
    fun slaveStep() =
        stepBuilderFactory["slaveStep"]
                .chunk<SourceRecord, Record>(chunkSize.toInt())
                .reader(itemReader)
                .faultTolerant()
                .skip(MissingFieldException::class.java)
                .skip(DecryptionFailureException::class.java)
                .skip(BadDecryptedDataException::class.java)
                .skipLimit(Integer.MAX_VALUE)
                .retry(NoSuchColumnFamilyException::class.java)
                .retryLimit(retryLimit.toInt())
                .processor(itemProcessor())
                .writer(itemWriter)
                .build()


    @Bean
    fun step() =
        stepBuilderFactory.get("step")
            .chunk<SourceRecord, Record>(chunkSize.toInt())
            .reader(itemReader)
            .faultTolerant()
            .skip(MissingFieldException::class.java)
            .skip(DecryptionFailureException::class.java)
            .skip(BadDecryptedDataException::class.java)
            .skipLimit(Integer.MAX_VALUE)
            .processor(itemProcessor())
            .writer(itemWriter)
            .build()


    @Bean
    fun taskExecutor() = SimpleAsyncTaskExecutor("htme").apply {
        concurrencyLimit = Integer.parseInt(threadCount)
    }

    @Bean
    @StepScope
    fun itemProcessor(): ItemProcessor<SourceRecord, Record> =
        CompositeItemProcessor<SourceRecord, Record>().apply {
            setDelegates(listOf(decryptionProcessor, sanitisationProcessor))
        }

    @Autowired
    lateinit var partitioner: Partitioner

    @Autowired
    lateinit var itemReader: ItemReader<SourceRecord>

    @Autowired
    lateinit var decryptionProcessor: ItemProcessor<SourceRecord, DecryptedRecord>

    @Autowired
    lateinit var sanitisationProcessor: ItemProcessor<DecryptedRecord, Record>

    @Autowired
    lateinit var itemWriter: ItemWriter<Record>

    @Autowired
    lateinit var jobBuilderFactory: JobBuilderFactory

    @Autowired
    lateinit var stepBuilderFactory: StepBuilderFactory

    @Value("\${chunk.size:10000}")
    lateinit var chunkSize: String

    @Value("\${thread.count:256}")
    lateinit var threadCount: String

    @Value("\${scan.width:5}")
    private lateinit var scanWidth: String


    @Value("\${retry.limit:50}")
    private lateinit var retryLimit: String
}
