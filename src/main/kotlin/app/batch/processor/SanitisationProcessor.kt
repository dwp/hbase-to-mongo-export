package app.batch.processor

import app.exceptions.DataKeyServiceUnavailableException
import com.google.gson.JsonObject
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

// See DA-2374
@Component
class SanitisationProcessor: ItemProcessor<JsonObject, String> {

    @Throws(DataKeyServiceUnavailableException::class)
    override fun process(item: JsonObject): String? {
        val output = sanitiseCollectionSpecific(item)
        return output.replace("$", "d_")
                .replace("\u0000", "")
                .replace("\\u0000", "")
                .replace("_archivedDateTime", "_removedDateTime")
                .replace("_archived", "_removed")
    }

    fun sanitiseCollectionSpecific(input: JsonObject): String {
        val db = input.getAsJsonObject("message")?.getAsJsonPrimitive("db")?.asString
        val collection = input .getAsJsonObject("message")?.getAsJsonPrimitive("collection")?.asString
        if((db == "penalties-and-deductions" && collection == "sanction")
                || (db == "core" && collection == "healthAndDisabilityDeclaration")
                || (db == "accepted-data" && collection == "healthAndDisabilityCircumstances")) {
            logger.debug("Sanitising output for db: {} and collection: {}", db, collection)
            return input.toString().replace("\r", "").replace("\n", "")
                    .replace("\\r", "").replace("\\n", "")
        }
        return input.toString()
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(SanitisationProcessor::class.toString())
    }
}