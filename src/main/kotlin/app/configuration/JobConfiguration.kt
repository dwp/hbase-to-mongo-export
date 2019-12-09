package app.configuration

import app.domain.DecryptedRecord
import app.domain.Record
import app.domain.SourceRecord
import app.exceptions.BadDecryptedDataException
import app.exceptions.DecryptionFailureException
import app.exceptions.MissingFieldException
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.support.CompositeItemProcessor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration
@Profile("batchRun")
@EnableBatchProcessing
class JobConfiguration : DefaultBatchConfigurer() {

    @Bean
    fun importUserJob(listener: JobCompletionNotificationListener, step: Step) =
        jobBuilderFactory.get("nightlyExportBatchJob")
            .incrementer(RunIdIncrementer())
            .listener(listener)
            .flow(step)
            .end()
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

    fun itemProcessor(): ItemProcessor<SourceRecord, Record> =
        CompositeItemProcessor<SourceRecord, Record>().apply {
            setDelegates(listOf(decryptionProcessor, sanitisationProcessor))
        }

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
}
