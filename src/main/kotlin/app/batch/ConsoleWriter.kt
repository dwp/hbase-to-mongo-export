package app.batch

import org.springframework.batch.item.ItemWriter
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Component
@Profile("consoleOutput")
class ConsoleWriter: ItemWriter<String> {
    override fun write(items: MutableList<out String>) {
        items.forEach {
            println(it)
        }
    }
}