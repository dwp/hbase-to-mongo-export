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
        val width = scanWidth.toInt()

        for (i in Byte.MIN_VALUE..0 step width) {
            val start = i
            val stop = if (i + width > 0) 0 else (i + width)
            if (start < stop) {
                map.put("p${start}-${stop}", ExecutionContext().apply {
                    putInt("start", start)
                    putInt("stop", stop)
                })
            }
        }


        for (i in 0..Byte.MAX_VALUE step width) {
            val start = i
            val stop: Byte = if (i + width > Byte.MAX_VALUE) Byte.MIN_VALUE else (i + width).toByte()
            if (start.toByte() != stop) {
                map.put("p${start}-${stop}", ExecutionContext().apply {
                    putInt("start", start)
                    putInt("stop", stop.toInt())
                })
            }
        }

        return map
    }

    @Value("\${scan.width:5}")
    private lateinit var scanWidth: String

}
