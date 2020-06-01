package app.utils

import com.google.gson.Gson
import com.google.gson.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Test

class DateWrapperTest {

    val dateOne = "2019-12-14T15:01:02.000+0000"
    val dateTwo = "2018-12-14T15:01:02.000+0000"
    val dateThree = "2017-12-14T15:01:02.000+0000"
    val dateFour = "2016-12-14T15:01:02.000+0000"
    val formattedDateOne = "2019-12-14T15:01:02.000Z"
    val formattedDateTwo = "2018-12-14T15:01:02.000Z"
    val formattedDateThree = "2017-12-14T15:01:02.000Z"
    val formattedDateFour = "2016-12-14T15:01:02.000Z"

    @Test
    fun processesDeepDates() {
        val json = """{
            |   parentDate: "2017-12-14T15:01:02.000+0000",
            |   childObjectWithDates: {
            |       "grandChildObjectWithDate": {
            |           "grandChildDate1": "2019-12-14T15:01:02.000+0000"
            |       },
            |       "childDate": "2018-12-14T15:01:02.000+0000",
            |       arrayWithDates: [
            |           "2010-12-14T15:01:02.000+0000",
            |           [
            |               "2011-12-14T15:01:02.000+0000"
            |           ],
            |           {
            |               grandChildDate3: "2012-12-14T15:01:02.000+0000"
            |           }
            |       ]
            |   }
            |}
        """.trimMargin()

        val jsonObject = Gson().fromJson(json, JsonObject::class.java)
        DateWrapper().processJsonObject(jsonObject)

        val wrapped = """{
            |   "parentDate": {
            |       d_date: "2017-12-14T15:01:02.000Z"
            |   },
            |   "childObjectWithDates": {
            |       "grandChildObjectWithDate": {
            |           "grandChildDate1": {
            |               "d_date": "2019-12-14T15:01:02.000Z"
            |           }
            |       },
            |       "childDate": {
            |           "d_date": "2018-12-14T15:01:02.000Z"
            |       },
            |       arrayWithDates: [
            |           { d_date: "2010-12-14T15:01:02.000Z" },
            |           [
            |               { d_date: "2011-12-14T15:01:02.000Z" }
            |           ],
            |           {
            |               grandChildDate3: { d_date: "2012-12-14T15:01:02.000Z" }
            |           }
            |       ]
            |    }
            |}
        """.trimMargin()
        val wrappedObject = Gson().fromJson(wrapped, JsonObject::class.java)
        assertEquals(wrappedObject, jsonObject)
    }


}
