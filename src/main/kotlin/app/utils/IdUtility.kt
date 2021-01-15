package app.utils

import com.google.gson.Gson
import com.google.gson.JsonObject

object IdUtility {
    fun reverseEngineerId(hbaseId: String): Pair<String, String> {
        val jsonObject = gson.fromJson(hbaseId, JsonObject::class.java)
        return when {
            wasScalarId(jsonObject) -> {
                Pair(jsonObject["id"].asString, jsonUtils.sortJsonByKey("""{"${'$'}oid":"${jsonObject["id"].asString}"}"""))
            }
            else -> {
                val sorted = jsonUtils.sortJsonByKey(jsonObject.toString())
                Pair(sorted, sorted)
            }
        }
    }

    private fun wasScalarId(id: JsonObject): Boolean {
        return id.keySet().size == 1 && id.has("id")
    }

    private val gson = Gson()
    private val jsonUtils: JsonUtils = JsonUtils()
}
