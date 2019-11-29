package app.batch.processor

import app.domain.DecryptedRecord
import app.domain.Record
import app.exceptions.DataKeyServiceUnavailableException
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
        logger.debug("Sanitized record : ${manifestRecord.id} ${manifestRecord.timestamp}")
        return Record(replacedOutput, manifestRecord)
    }

    fun sanitiseCollectionSpecific(input: DecryptedRecord): String {
        val db = input.manifestRecord.db
        val collection = input.manifestRecord.collection
        val dbObject = input.dbObject
        if ((db == "penalties-and-deductions" && collection == "sanction")
            || (db == "core" && collection == "healthAndDisabilityDeclaration")
            || (db == "accepted-data" && collection == "healthAndDisabilityCircumstances")) {
            logger.debug("Sanitising output for db: {} and collection: {}", db, collection)
            return dbObject.toString().replace(replacementRegex, "")
        }
        return dbObject.toString()
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(SanitisationProcessor::class.toString())
    }
}
