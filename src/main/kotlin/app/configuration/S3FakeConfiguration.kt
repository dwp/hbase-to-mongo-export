package app.configuration

import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration
@Profile("fakeS3Client")
class S3FakeConfiguration {

    @Bean
    fun amazonS3(): AmazonS3 {

        // eu-west-1 -> EU_WEST_2 (i.e tf style to enum name)
        val updatedRegion = region.toUpperCase().replace("-", "_")
        val clientRegion = Regions.valueOf(updatedRegion)

        //This code expects that you have AWS credentials set up per:
        // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
        println("******************************Environment Vars*****************************")
        println(System.getenv("SystemRoot"))

        //val creds = BasicAWSCredentials("not_set", "not_set")
        //return  AmazonS3ClientBuilder.standard().withCredentials(AWSStaticCredentialsProvider(creds)).withRegion("us-east-1").build()
        /*return AmazonS3ClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain())
                .withRegion("us-east-1")
                .build()*/
        //amazonS3.setEndpoint("http://localhost:4572")
        return AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://localhost:4572", "eu-west-2"))
            .withClientConfiguration(ClientConfiguration().withProtocol(Protocol.HTTP))
            .withCredentials(
                AWSStaticCredentialsProvider(BasicAWSCredentials("not_set", "not_set")))
            .withPathStyleAccessEnabled(true)
            .disableChunkedEncoding()
            .build()
    }

    @Value("\${aws.region}")
    private lateinit var region: String

}