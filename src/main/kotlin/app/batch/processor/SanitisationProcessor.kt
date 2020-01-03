package app.batch.processor

import app.domain.DecryptedRecord
import app.domain.Record
import app.exceptions.DataKeyServiceUnavailableException
import app.utils.logging.logDebug
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

// See https://projects.ucd.gpn.gov.uk/browse/DW-2374
@Component
class SanitisationProcessor : ItemProcessor<DecryptedRecord, Record> {

    val replacementRegex = """(?<!\\)\\[r|n]""".toRegex()

    @Throws(DataKeyServiceUnavailableException::class)
    override fun process(item: DecryptedRecord): Record? {
        val output = sanitiseCollectionSpecific(item)
        val replacedOutput = output.replace("$", "d_")
            .replace("\\u0000", "")
            .replace("_archivedDateTime", "_removedDateTime")
            .replace("_archived", "_removed")

        val manifestRecord = item.manifestRecord
        logDebug(logger, "Sanitized record", "manifest_record_id", manifestRecord.id, "manifest_record_timestamp", "${manifestRecord.timestamp}")
        return Record(replacedOutput, manifestRecord)
    }

    fun sanitiseCollectionSpecific(input: DecryptedRecord): String {
        val db = input.manifestRecord.db
        val collection = input.manifestRecord.collection
        val dbObject = input.dbObject
        if ((db == "penalties-and-deductions" && collection == "sanction")
            || (db == "core" && collection == "healthAndDisabilityDeclaration")
            || (db == "accepted-data" && collection == "healthAndDisabilityCircumstances")) {
            logDebug(logger, "Sanitising output", "db_name", db, "collection_name", collection)
            return dbObject.toString().replace(replacementRegex, "")
        }
        return dbObject.toString()
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(SanitisationProcessor::class.toString())
    }
}
