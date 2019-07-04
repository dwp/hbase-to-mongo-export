package uk.gov.dwp.dataworks.export

import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@SpringBootTest
@ActiveProfiles("phoneyServices", "unitTest")
class DataworksExportApplicationTests {

	@Test
	fun contextLoads() {
	}


}
