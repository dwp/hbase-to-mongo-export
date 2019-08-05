package app.batch

import app.services.CipherService
import app.services.KeyService
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.*
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

@Component
@Profile("outputToConsole")
class ConsoleWriter : ItemWriter<String> {
    override fun write(items: MutableList<out String>) {
        items.forEach {
            println(it)
        }
    }
}
