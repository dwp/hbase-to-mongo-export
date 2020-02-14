package app.utils

class TextUtils {
    fun topicNameTableMatcher(topicName: String) = qualifiedTablePattern.find(topicName)
    private val qualifiedTablePattern = Regex("""^\w+\.([-\w]+)\.([-\w]+)$""")
}
