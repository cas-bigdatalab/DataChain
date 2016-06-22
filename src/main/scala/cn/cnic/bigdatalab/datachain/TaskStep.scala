package cn.cnic.bigdatalab.datachain

import cn.cnic.bigdatalab.Task.{StoreTask, OfflineTask, RealTimeTask, BaseTask}

/**
  * Created by cnic on 2016/6/21.
  */
class TaskStep() {
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

}

