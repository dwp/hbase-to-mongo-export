import app.configuration.ContextConfiguration
import app.configuration.LocalStackConfiguration
import app.services.impl.HttpKeyService
import org.springframework.boot.test.autoconfigure.properties.PropertyMapping
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.PropertySource
import org.springframework.test.context.TestPropertySource

@Configuration
@Import(LocalStackConfiguration::class, ContextConfiguration::class)
@ComponentScan("app.utils", "app.services.impl")
@PropertySource("classpath:integration.properties")
class TestConfiguration

