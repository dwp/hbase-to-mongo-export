package app.utils

import org.junit.Test
import io.kotlintest.shouldBe

class JsonUtilsTest {
    @Test
    fun Should_Sort_Json_By_Key_Name() {
        val jsonStringUnsorted = "{\"testA\":\"test1\", \"testC\":2, \"testB\":true}"
        val jsonStringSorted = "{\"testA\":\"test1\",\"testB\":true,\"testC\":2}"

        val sortedJson = JsonUtils().sortJsonByKey(jsonStringUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    @Test
    fun Should_Sort_Json_By_Key_Name_Case_Sensitively() {
        val jsonStringUnsorted = "{\"testb\":true, \"testA\":\"test1\", \"testC\":2}"
        val jsonStringSorted = "{\"testA\":\"test1\",\"testC\":2,\"testb\":true}"

        val sortedJson = JsonUtils().sortJsonByKey(jsonStringUnsorted)

        sortedJson shouldBe jsonStringSorted
    }
}

