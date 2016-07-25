package cn.cnic.bigdatalab.entity

/**
  * Created by Flora on 2016/7/25.
  */
class Entry {
  var mainClass: String = _
  var menthodName: String = _
  var language: String = _

}

object Entry{
  def parserMap(map: Map[String, Any]):Entry={

    val entry = new Entry()

    //mainClass
    assert(!map.get("mainClass").get.asInstanceOf[String].isEmpty)
    entry.mainClass = map.get("mainClass").get.asInstanceOf[String]


    //methodName
    assert(!map.get("methodName").get.asInstanceOf[String].isEmpty)
    entry.menthodName = map.get("methodName").get.asInstanceOf[String]

    //language
    assert(!map.get("language").get.asInstanceOf[String].isEmpty)
    entry.language = map.get("language").get.asInstanceOf[String]

    entry

  }
}
