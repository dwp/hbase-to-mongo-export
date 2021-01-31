
import app.configuration.ContextConfiguration
import app.configuration.LocalStackConfiguration
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.PropertySource

@Configuration
@Import(LocalStackConfiguration::class, ContextConfiguration::class)
@ComponentScan("app.utils", "app.services.impl", "app.configuration")
@PropertySource("classpath:integration.properties")
class TestConfiguration

