package cn.cnic.bigdatalab.datachain

import java.util


/**
  * Created by cnic on 2016/6/21.
  */
class Chain {

  private val stepList: util.ArrayList[Step] =  new util.ArrayList[Step]()

  def addStep(step: Step): Chain = {
    this.stepList.add(step)
    this
  }

  def run()={
    for(i <- 0 to stepList.size() -1 ){
      stepList.get(i).run
    }
  }

}
