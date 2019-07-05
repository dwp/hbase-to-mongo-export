package app.batch

import com.google.gson.Gson
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import app.domain.SourceRecord

@Component
class JsonProcessor: ItemProcessor<SourceRecord, String> {
    override fun process(item: SourceRecord): String = Gson().toJson(item)
}