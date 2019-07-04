package uk.gov.dwp.dataworks.export.batch

import com.google.gson.Gson
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.export.domain.SourceRecord

@Component
class JsonProcessor: ItemProcessor<SourceRecord, String> {
    override fun process(item: SourceRecord): String = Gson().toJson(item)
}