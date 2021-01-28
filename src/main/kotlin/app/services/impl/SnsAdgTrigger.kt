package app.services.impl

import app.services.AdgTrigger
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.model.PublishRequest
import com.amazonaws.services.sns.model.PublishResult
import org.springframework.stereotype.Component

@Component
class SnsAdgTrigger(private val sns: AmazonSNS): AdgTrigger {

    override fun triggerAdg() {
        val wtf: PublishResult = sns.publish(PublishRequest().apply {
            topicArn = "arn:aws:sns:us-east-1:000000000000:trigger-adg-topic"
            message = "poo"
        })
        println("WOOOOOOOOOOOOOOOOOOOOOOOOOOOO got for it ADG: $wtf.")
    }
}
