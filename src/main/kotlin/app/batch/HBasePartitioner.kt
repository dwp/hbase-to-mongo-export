package app.batch

import org.springframework.batch.core.partition.support.Partitioner
import org.springframework.batch.item.ExecutionContext
import org.springframework.stereotype.Component
import java.util.*

@Component
class HBasePartitioner: Partitioner {
    override fun partition(gridSize: Int): MutableMap<String, ExecutionContext> {
        val map: MutableMap<String, ExecutionContext> = HashMap(gridSize)
        for (i in 0..gridSize) {
            val ec = ExecutionContext()
            ec.putString("prefix", "000$i")
            map.put("p$i", ec)
        }
        return map
    }
}
