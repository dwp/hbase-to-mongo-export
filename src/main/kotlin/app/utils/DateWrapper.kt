package app.utils

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import java.text.SimpleDateFormat
import java.util.*

class DateWrapper {

    fun processJsonObject(jsonObject: JsonObject, includeLastModified: Boolean = true) {
        jsonObject.keySet()
                .filter {
                    it != "_lastModifiedDateTime" || includeLastModified
                }
                .forEach { key ->
                    key?.let {
                        processJsonElement(jsonObject, it, jsonObject[it])
                    }
                }
    }

    private fun processJsonElement(parent: JsonObject, key: String, child: JsonElement) {
        when {
            isMongoDateObject(child) -> {
                processMongoDate(child.asJsonObject)
            }
            child.isJsonObject -> {
                processJsonObject(child.asJsonObject)
            }
            child.isJsonArray -> {
                processJsonArray(child.asJsonArray)
            }
            child.isJsonPrimitive && child.asJsonPrimitive.isString -> {
                processJsonPrimitive(parent, key, child.asJsonPrimitive.asString)
            }
        }
    }

    private fun processMongoDate(mongoDateObject: JsonObject) {
        val timestamp = mongoDateObject[dateFieldKey].asString
        parsedDate(timestamp)?.let { date ->
            mongoDateObject.remove(dateFieldKey)
            mongoDateObject.addProperty(dateFieldKey, SimpleDateFormat(outgoingFormat).format(date))
        }
    }

    private fun isMongoDateObject(jsonElement: JsonElement?) =
            jsonElement != null &&
                    jsonElement.isJsonObject &&
                    jsonElement.asJsonObject.size() == 1 &&
                    jsonElement.asJsonObject[dateFieldKey] != null &&
                    jsonElement.asJsonObject[dateFieldKey].isJsonPrimitive


    private fun processJsonArray(jsonArray: JsonArray) {
        for (i in 0 until jsonArray.size()) {
            val value = jsonArray[i]
            when {
                value.isJsonObject -> {
                    processJsonObject(value.asJsonObject)
                }
                value.isJsonArray -> {
                    processJsonArray(value.asJsonArray)
                }
                value.isJsonPrimitive && value.asJsonPrimitive.isString -> {
                    parsedDate((value.asJsonPrimitive.asString))?.let { date ->
                        jsonArray.set(i, dateObject(date))
                    }
                }
            }
        }
    }

    private fun processJsonPrimitive(jsonObject: JsonObject, key: String, value: String) =
            parsedDate(value)?.let { date ->
                replaceDateWithObject(jsonObject, key, date)
            }

    private fun replaceDateWithObject(jsonObject: JsonObject, key: String, date: Date) =
            jsonObject.add(key, dateObject(date))

    private fun dateObject(date: Date): JsonObject {
        val dateObject = JsonObject()
        dateObject.addProperty(dateFieldKey, SimpleDateFormat(outgoingFormat).format(date))
        return dateObject
    }

    private fun parsedDate(string: String) =
            if (incomingRe.matches(string)) {
                SimpleDateFormat(incomingFormat).parse(string)
            }
            else if (outgoingRe.matches(string)) {
                SimpleDateFormat(outgoingFormat).parse(string)
            }
            else {
                null
            }


    private val incomingFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"
    private val outgoingFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    private val dateFieldKey = "\$date"
    private val incomingRe = Regex("""\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+\d{4}""")
    private val outgoingRe = Regex("""\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z""")
}
