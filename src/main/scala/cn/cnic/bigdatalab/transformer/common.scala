package com.github.casbigdatalab.datachain.transformer

import java.util

/**
 * Created by cnic-liliang on 2016/5/25.
 */

trait common {
  def transform(msg:String): util.ArrayList[String]
}

