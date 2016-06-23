package cn.cnic.bigdatalab.Task

import cn.cnic.bigdatalab.entity.Schema
import com.github.casbigdatalab.datachain.transformer.Transformer

/**
  * Created by cnic on 2016/6/21.
  */
abstract class BaseTask() {
  def run

}

class RealTimeTask(sql:String, topic:String, schema:Schema, transformer: Transformer) extends BaseTask(){
  override def run(): Unit ={

  }
}
class OfflineTask(sql:String, topic:String, schema:Schema, transformer: Transformer) extends BaseTask(){
  override def run(): Unit ={

  }

}

class StoreTask(topic:String, schema:Schema, transformer: Transformer) extends BaseTask(){
  override def run(): Unit ={
    //create task
    //scheduler.deploy


  }

}