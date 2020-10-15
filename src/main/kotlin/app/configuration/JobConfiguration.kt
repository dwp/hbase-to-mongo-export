package app.configuration

import app.batch.JobCompletionNotificationListener
import app.domain.DecryptedRecord
import app.domain.Record
import app.domain.SourceRecord
import app.exceptions.BadDecryptedDataException
import app.exceptions.DecryptionFailureException
import app.exceptions.MissingFieldException
import com.amazonaws.services.dynamodbv2.model.TableNotFoundException
import org.apache.hadoop.hbase.client.Result
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
import org.springframework.retry.policy.SimpleRetryPolicy
import java.io.IOException

@Configuration
@Profile("batchRun")
@EnableBatchProcessing
class JobConfiguration : DefaultBatchConfigurer() {

    @Bean
    fun importUserJob(jobCompletionNotificationListener: JobCompletionNotificationListener) =
            jobBuilderFactory.get("nightlyExportBatchJob")
                    .incrementer(RunIdIncrementer())
                    .listener(jobCompletionNotificationListener)
                    .flow(step1())
                    .end()
                    .build()

    @Bean
    fun step1() = stepBuilderFactory["step1"]
                .partitioner(slaveStep().name, partitioner)
                .step(slaveStep())
                .gridSize(256 / scanWidth.toInt())
                .taskExecutor(taskExecutor())
                .build()

    @Bean
    fun slaveStep() = stepBuilderFactory["slaveStep"]
            .chunk<Result, Record>(chunkSize.toInt())
                .reader(itemReader)
                .faultTolerant()
                .retry(IOException::class.java)
                .retryPolicy(SimpleRetryPolicy().apply {
                    maxAttempts = maxRetries.toInt()
                })
                .noSkip(TableNotFoundException::class.java)
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
    fun itemProcessor(): ItemProcessor<Result, Record> =
            CompositeItemProcessor<Result, Record>().apply {
                setDelegates(listOf(resultProcessor, decryptionProcessor, sanitisationProcessor))
            }

    @Autowired
    lateinit var partitioner: Partitioner

    @Autowired
    lateinit var itemReader: ItemReader<Result>

    @Autowired
    lateinit var resultProcessor: ItemProcessor<Result, SourceRecord>

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

    @Value("\${chunk.size:1000}")
    lateinit var chunkSize: String

    @Value("\${thread.count:256}")
    lateinit var threadCount: String

    @Value("\${scan.width:5}")
    private lateinit var scanWidth: String

    @Value("\${s3.max.retries:20}")
    private lateinit var maxRetries: String
}
