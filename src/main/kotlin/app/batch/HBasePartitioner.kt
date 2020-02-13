package app.batch

import org.springframework.batch.core.partition.support.Partitioner
import org.springframework.batch.item.ExecutionContext
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.*

@Component
class HBasePartitioner: Partitioner {
    override fun partition(gridSize: Int): MutableMap<String, ExecutionContext> {
        val map: MutableMap<String, ExecutionContext> = HashMap(gridSize)

        for (i in -128..-1 step scanWidth.toInt()) {
            map.put("p${i + 256}", ExecutionContext().apply {
                putInt("start", i)
                putInt("stop", i + 1)
            })
        }

        for (i in 0..127 step scanWidth.toInt()) {
            map.put("p$i", ExecutionContext().apply {
                putInt("start", i)
                putInt("stop", i + 1)
            })
        }


        return map
    }

    @Value("\${scan.width:5}")
    private lateinit var scanWidth: String

}
