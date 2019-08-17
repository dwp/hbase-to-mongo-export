package app.configuration

import org.apache.http.client.HttpClient
import org.apache.http.impl.client.CloseableHttpClient
import java.io.Closeable

interface HttpClientProvider {
    fun client(): CloseableHttpClient
}