package cn.cnic.bigdatalab.Task

import cn.cnic.bigdatalab.entity.Schema
import cn.cnic.bigdatalab.utils.PropertyUtil
import com.github.casbigdatalab.datachain.transformer.Transformer

/**
  * Created by cnic on 2016/6/21.
  */
abstract class BaseTask() {
  protected var scheduler: Scheduler = _
  def run

}

class RealTimeTask(taskInstance: TaskInstance ) extends BaseTask(){
  override def run(): Unit ={
    this.scheduler = new RealTimeScheduler
    scheduler.deploy(taskInstance)

  }
}
class OfflineTask(taskInstance: TaskInstance) extends BaseTask(){
  override def run(): Unit ={
    this.scheduler = new OfflineScheduler
    scheduler.deploy(taskInstance)
  }
}

class StoreTask(taskInstance: TaskInstance) extends BaseTask(){
  override def run(): Unit ={
    //create task
    //scheduler.deploy


  }

}