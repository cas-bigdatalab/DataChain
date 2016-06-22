package cn.cnic.bigdatalab.utils

import org.apache.log4j.{Level, Logger}

import org.apache.spark.Logging

/**
  * Created by Flora on 2016/5/6.
  */
object StreamingLogLevels extends Logging{

  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }

}
