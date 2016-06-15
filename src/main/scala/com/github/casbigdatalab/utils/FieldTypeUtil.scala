package com.github.casbigdatalab.utils

import org.apache.spark.sql.types.{DataType, DataTypes}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by duyuanyuan on 2016/6/13.
  */
object FieldTypeUtil {
  def stringToDataType(str : String): DataType = str.toLowerCase match{
    case "string" => DataTypes.StringType
    //    case "binary" => DataTypes.BinaryType
    case "byte" => DataTypes.ByteType
    case "boolean" => DataTypes.BooleanType
    case "double" => DataTypes.DoubleType
    case "date" => DataTypes.DateType
    case "float" => DataTypes.FloatType
    case "integer" => DataTypes.IntegerType
    case "int" => DataTypes.IntegerType
    case "long" => DataTypes.LongType
    case "null" => DataTypes.NullType
    case "short" => DataTypes.ShortType
    case "timestamp" => DataTypes.TimestampType
//    case "calendarinterval" => DataTypes.CalendarIntervalType
    case _ => throw new IllegalArgumentException("field could not find the matched DataType.")
  }

  def parseDataType(str : String, value : String) : Any = str.toLowerCase match {
    case "string" => value
    case "byte" => value.toByte
    case "boolean" => value.toBoolean
    case "double" => value.toDouble
    case "date" => DateTime.parse(value, DateTimeFormat.forPattern("yyyy-MM-dd H:mm:ss"))
    case "float" => value.toFloat
    case "integer" => value.toInt
    case "int" => value.toInt
    case "long" => value.toLong
    case "null" => None
    case "short" => value.toShort
    case "timestamp" => DateTime.parse(value, DateTimeFormat.forPattern("yyyy-MM-dd H:mm:ss"))
    //    case "calendarinterval" => DataTypes.CalendarIntervalType
    case _ => throw new IllegalArgumentException("field could not parse data type.")
  }
}
