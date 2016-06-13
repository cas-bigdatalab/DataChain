name := "DataChain"

version := "1.0"

scalaVersion := "2.10.4"

val hadoopVersion = "2.6.0"

val sparkVersion = "1.6.1"

libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",
  "org.apache.spark" %% "spark-streaming" %  sparkVersion % "compile",
  "org.apache.spark" %% "spark-catalyst" %  sparkVersion % "compile",
  "org.apache.spark" %% "spark-hive" %  sparkVersion % "compile",
  "org.apache.spark" %% "spark-streaming-kafka" %  sparkVersion % "compile",
  "org.apache.kafka" % "kafka_2.10" % "0.8.2.2",
  "mysql" % "mysql-connector-java" % "5.1.39",
  "org.apache.zookeeper" % "zookeeper" % "3.4.5" exclude("org.jboss.netty", "netty"),
  "com.yammer.metrics" % "metrics-core" % "2.2.0",
  "org.apache.commons" % "commons-csv" % "1.4",
  "joda-time" % "joda-time" % "2.9.3"
)

fork in Test := true
