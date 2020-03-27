package app.utils

import org.springframework.stereotype.Component

@Component
class TextUtils {
    fun topicNameTableMatcher(topicName: String) = qualifiedTablePattern.find(topicName)
    private val qualifiedTablePattern = Regex("""^\w+\.([-\w]+)\.([-\w]+)$""")
}
