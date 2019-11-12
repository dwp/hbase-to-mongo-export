package app.configuration

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.DeleteObjectRequest
import org.slf4j.LoggerFactory
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.StepExecutionListener
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

//@Component
class BeforeStepListener : StepExecutionListener {
    @Autowired
    private lateinit var s3Client: AmazonS3

    @Value("\${s3.bucket}")
    private lateinit var s3BucketName: String

    @Value("\${s3.manifest.prefix.folder}")
    private lateinit var s3PrefixFolder: String

    override fun beforeStep(stepExecution: StepExecution) {

       val objectSummaries =  s3Client.listObjectsV2(s3BucketName, s3PrefixFolder).objectSummaries
       objectSummaries.forEach{
           // TODO verify if the key is the full path or just the name of the file
           val deleteObjectRequest = DeleteObjectRequest(s3BucketName,it.key)
           s3Client.deleteObject(deleteObjectRequest)
       }
    }

    override fun afterStep(stepExecution: StepExecution): ExitStatus? {
       return  stepExecution.exitStatus
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BeforeStepListener::class.java)
    }
}
