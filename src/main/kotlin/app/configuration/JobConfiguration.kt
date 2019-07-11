package app.configuration

import app.domain.SourceRecord
import app.exceptions.MissingFieldException
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.core.task.TaskExecutor

@Configuration
@EnableBatchProcessing
class JobConfiguration: DefaultBatchConfigurer() {

    @Bean
    fun importUserJob(listener: JobCompletionNotificationListener, step: Step): Job {
        return jobBuilderFactory.get("nightlyExportBatchJob")
                .incrementer(RunIdIncrementer())
                .listener(listener)
                .flow(step)
                .end()
                .build()
    }

    @Bean
    fun step() = stepBuilderFactory.get("step")
            .chunk<SourceRecord, String>(2)
            .reader(itemReader)
            .faultTolerant()
            .skip(MissingFieldException::class.java)
            .skipLimit(Integer.MAX_VALUE)
            .processor(itemProcessor)
            .writer(itemWriter)
            .build()

    @Autowired
    lateinit var itemReader: ItemReader<SourceRecord>

    @Autowired
    lateinit var itemProcessor: ItemProcessor<SourceRecord, String>

    @Autowired
    lateinit var itemWriter: ItemWriter<String>

    @Autowired
    lateinit var jobBuilderFactory: JobBuilderFactory

    @Autowired
    lateinit var stepBuilderFactory: StepBuilderFactory

}