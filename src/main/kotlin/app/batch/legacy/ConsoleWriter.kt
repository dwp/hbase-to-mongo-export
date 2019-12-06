package app.batch.legacy

import app.domain.Record
import org.springframework.batch.item.ItemWriter
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Component
@Profile("outputToConsole")
class ConsoleWriter : ItemWriter<Record> {
    override fun write(items: MutableList<out Record>) {
        items.forEach {
            println(it.dbObjectAsString)
        }
    }
}
