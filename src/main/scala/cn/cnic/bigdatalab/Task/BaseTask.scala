package cn.cnic.bigdatalab.Task

/**
  * Created by cnic on 2016/6/21.
  */
class BaseTask(sql:String) {

}

class RealTimeTask(sql:String, topic:String) extends BaseTask(sql:String){

}
class OfflineTask(sql:String) extends BaseTask(sql:String){

}

class StoreTask(sql:String) extends BaseTask(sql:String){

}