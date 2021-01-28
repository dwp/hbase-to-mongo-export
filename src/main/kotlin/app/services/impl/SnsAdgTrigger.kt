package app.services.impl

import app.services.AdgTrigger
import org.springframework.stereotype.Component

@Component
class SnsAdgTrigger: AdgTrigger {
    override fun triggerAdg() {
        println("WOOOOOOOOOOOOOOOOOOOOOOOOOOOO got for it ADG.")
    }
}
