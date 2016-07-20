package cn.cnic.bigdatalab.datachain

import cn.cnic.bigdatalab.task.{StoreTask, OfflineTask, RealTimeTask, BaseTask}

/**
  * Created by cnic on 2016/6/21.
  */
class TaskStep() extends Step{
  var task: BaseTask = _

  def setRealTimeTask(task:RealTimeTask): TaskStep ={

    this.task = task
    this
  }

  def setOfflineTask(task:OfflineTask): TaskStep ={

    this.task = task
    this
  }

  def setStoreTask(task:StoreTask): TaskStep ={
    this.task = task
    this
  }

  override def run: Unit = {
    this.task.run
  }
}

