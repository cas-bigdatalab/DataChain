package cn.cnic.bigdatalab.entity

/**
  * Created by Flora on 2016/6/23.
  */
class Schema {
  private var driver:String = _
  private var db:String = _
  private var table:String = _
  private var columns:Map[String, String] = _

  def getDriver(): String ={
    driver
  }

  def setDriver(driver: String): Unit ={
    this.driver = driver
  }

  def getDb(): String ={
    db
  }

  def setDb(db: String): Unit ={
    this.db = db
  }

  def getTable(): String ={
    table
  }

  def setTable(table: String): Unit ={
    this.table = table
  }

  def getColumns(): Map[String, String] ={
    columns
  }

  def setColumns(columns: Map[String, String]): Unit ={
    this.columns = columns
  }

}