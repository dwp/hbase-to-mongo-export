import groovy.json.JsonSlurper
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.PatternLayout
import spock.lang.Specification

class IntegrationTest extends Specification {

    Logger log

    def setup() {
        def appender = new ConsoleAppender()
        appender.with {
            layout = new PatternLayout("%d [%p|%c|%C{1}] %m%n")
            threshold = Level.INFO
            activateOptions()
        }
        Logger.getRootLogger().addAppender appender
        log = Logger.getLogger(IntegrationTest.class)
    }

    def "Writes the correct records"() {
        given: "hbase is up"
        when: "the table has been populated"
        and: "the process has run"

        File outputFile = new File("data/ucdata-file-output.txt")
        int attempts = 0
        log.info("${outputFile}: is file: ${outputFile.isFile()}")
        while (!outputFile.isFile() && ++attempts < 10) {
            log.info("Waiting for population process")
            sleep(3000)
        }
        assert(outputFile.isFile())

        then: "the latest records have been written"

        def line

        def reader = new BufferedReader(new FileReader(outputFile))
        def lineCount = 0
        while ((line = reader.readLine()) != null) {
            def jsonSlurper = new JsonSlurper()
            def object = jsonSlurper.parse(line.getBytes())
            println(object)
            println(object.getClass())
            def timestamp = object.get('timestamp')
            lineCount++
            assert timestamp == 10
        }
        lineCount == 7
    }
}