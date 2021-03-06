package app.configuration

import org.apache.http.client.config.RequestConfig
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.SSLContexts
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.File
import javax.net.ssl.SSLContext


@Component
class SecureHttpClientProvider : HttpClientProvider {

    override fun client(): CloseableHttpClient =
        HttpClients.custom().run {
            setDefaultRequestConfig(requestConfig())
            setSSLSocketFactory(connectionFactory())
            build()
        }


    private fun requestConfig(): RequestConfig =
        RequestConfig.custom().run {
            setConnectTimeout(connectTimeout.toInt())
            setConnectionRequestTimeout(connectionRequestTimeout.toInt())
            setSocketTimeout(socketTimeout.toInt())
            build()
        }


    private fun connectionFactory() = SSLConnectionSocketFactory(
        sslContext(),
        arrayOf("TLSv1.2"),
        null,
        SSLConnectionSocketFactory.getDefaultHostnameVerifier())

    private fun sslContext(): SSLContext =
        SSLContexts.custom().run {
            loadKeyMaterial(
                File(identityStore),
                identityStorePassword.toCharArray(),
                identityKeyPassword.toCharArray()) { _, _ -> identityStoreAlias }
            loadTrustMaterial(File(trustStore), trustStorePassword.toCharArray())
            build()
        }

    @Value("\${identity.keystore}")
    private lateinit var identityStore: String

    @Value("\${identity.store.password}")
    private lateinit var identityStorePassword: String

    @Value("\${identity.store.alias}")
    private lateinit var identityStoreAlias: String

    @Value("\${identity.key.password}")
    private lateinit var identityKeyPassword: String

    @Value("\${trust.keystore}")
    private lateinit var trustStore: String

    @Value("\${trust.store.password}")
    private lateinit var trustStorePassword: String

    @Value("\${connect.timeout:300000}")
    private lateinit var connectTimeout: String

    @Value("\${connection.request.timeout:300000}")
    private lateinit var connectionRequestTimeout: String

    @Value("\${socket.timeout:300000}")
    private lateinit var socketTimeout: String
}
