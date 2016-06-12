package com.github.casbigdatalab.datachain.dataparser

import java.util

/**
 * Created by cnic-liliang on 2016/5/25.
 */

trait common {
  def parse(msg:String): util.ArrayList[Map[String, Any]]
}

