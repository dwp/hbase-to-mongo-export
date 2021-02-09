
import app.configuration.ContextConfiguration
import app.configuration.LocalStackConfiguration
import app.configuration.SecureHttpClientProvider
import app.services.impl.AESCipherService
import app.services.impl.HttpKeyService
import io.prometheus.client.Counter
import org.springframework.context.annotation.*

@Configuration
@Import(LocalStackConfiguration::class, ContextConfiguration::class, HttpKeyService::class,
    SecureHttpClientProvider::class, AESCipherService::class)
@ComponentScan("app.utils")
@PropertySource("classpath:integration.properties")
class TestConfiguration {

    @Bean
    fun counter(): Counter = with(Counter.build()) {
        name("test_counter")
        help("Test help")
        register()
    }

}

