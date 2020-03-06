package app.configuration

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration
@Profile("realS3Client")
class S3RealConfiguration {

    @Bean
    fun amazonS3(): AmazonS3 {

        // eu-west-1 -> EU_WEST_2 (i.e tf style to enum name)
        val updatedRegion = region.toUpperCase().replace("-", "_")
        val clientRegion = Regions.valueOf(updatedRegion)

        val clientConfiguration = ClientConfiguration().apply {
            maxConnections = maximumS3Connections.toInt()
        }

        //This code expects that you have AWS credentials set up per:
        // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
        return AmazonS3ClientBuilder.standard()
            .withCredentials(DefaultAWSCredentialsProviderChain())
            .withRegion(clientRegion)
            .withClientConfiguration(clientConfiguration.withSocketTimeout(socketTimeOut.toInt()))
            .build()
    }

    @Value("\${aws.s3.max.connections:256}")
    private lateinit var maximumS3Connections: String

    @Value("\${aws.region}")
    private lateinit var region: String

    @Value("\${s3.socket.timeout:1800000}")
    private lateinit var socketTimeOut: String
}
