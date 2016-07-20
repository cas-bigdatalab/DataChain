import java.sql.{Date, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util.Locale

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by duyuanyuan on 2016/6/13.
  */
object ConvertColumn {

  def parseDataType(pre: PreparedStatement, i: Int, str : String, value : String) : PreparedStatement = str.toLowerCase match {
    case "string" => {
      pre.setString(i, value)
      pre
    }
    case "byte" => {
      pre.setByte(i, value.toByte)
      pre
    }
    case "boolean" => {
      pre.setBoolean(i, value.toBoolean)
      pre
    }
    case "double" => {
      pre.setDouble(i, value.toDouble)
      pre
    }
    case "date" => {
      pre.setDate(i, Date.valueOf(value))
      pre
    }
    case "float" => {
      pre.setFloat(i, value.toFloat)
      pre
    }
    case "integer" => {
      pre.setInt(i, value.toInt)
      pre
    }
    case "int" => {
      pre.setInt(i, value.toInt)
      pre
    }
    case "long" => {
      pre.setLong(i, value.toLong)
      pre
    }
    case "null" => {
      pre.setNull(i, i)
      pre
    }
    case "short" => {
      pre.setShort(i, value.toShort)
      pre
    }
    case "timestamp" => {
      pre.setTimestamp(i, new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value).getTime()))
      pre
    }
    case "datetype" => {
      pre.setDate(i, Date.valueOf(value))
      pre
    }
    //    case "calendarinterval" => DataTypes.CalendarIntervalType
    case _ => throw new IllegalArgumentException("field could not parse data type.")
  }

  def main(agrs: Array[String]): Unit ={

    val value = "1995-12-31 23:59:59"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val dt = sdf.parse(value)
    val ts = new Timestamp(dt.getTime)
    print(ts)
  }
}
