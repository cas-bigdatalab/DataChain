name := "DataChain"

version := "1.0"

scalaVersion := "2.10.4"

val hadoopVersion = "2.6.0"

val sparkVersion = "1.6.1"

val kiteVersion = "0.16.0"


libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion exclude("com.typesafe.akka", "akka-actor_2.10")
    exclude("com.typesafe.akka", "akka-remote_2.10") exclude("com.typesafe.akka", "akka-slf4j_2.10") excludeAll(ExclusionRule(organization = "org.eclipse.jetty")),
  "org.apache.spark" %% "spark-sql" % sparkVersion exclude("com.typesafe.akka", "akka-actor_2.10")
    exclude("com.typesafe.akka", "akka-remote_2.10") exclude("com.typesafe.akka", "akka-slf4j_2.10")
    excludeAll(ExclusionRule(organization = "org.eclipse.jetty")),
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile" ,
  "org.apache.spark" %% "spark-streaming" %  sparkVersion % "compile" exclude("com.typesafe.akka", "akka-actor_2.10")
    exclude("com.typesafe.akka", "akka-remote_2.10") exclude("com.typesafe.akka", "akka-slf4j_2.10")
    excludeAll(ExclusionRule(organization = "org.eclipse.jetty")),
  "org.apache.spark" %% "spark-catalyst" %  sparkVersion % "compile" exclude("com.typesafe.akka", "akka-actor_2.10")
    exclude("com.typesafe.akka", "akka-remote_2.10") exclude("com.typesafe.akka", "akka-slf4j_2.10")
    excludeAll(ExclusionRule(organization = "org.eclipse.jetty")),
  "org.apache.spark" %% "spark-hive" %  sparkVersion % "compile" exclude("com.typesafe.akka", "akka-actor_2.10")
    exclude("com.typesafe.akka", "akka-remote_2.10") exclude("com.typesafe.akka", "akka-slf4j_2.10")
    excludeAll(ExclusionRule(organization = "org.eclipse.jetty")),
  "org.apache.spark" %% "spark-streaming-kafka" %  sparkVersion % "compile" exclude("com.typesafe.akka", "akka-actor_2.10")
    exclude("com.typesafe.akka", "akka-remote_2.10") exclude("com.typesafe.akka", "akka-slf4j_2.10")
    excludeAll(ExclusionRule(organization = "org.eclipse.jetty")),
  "org.apache.spark" % "spark-mllib_2.10" % sparkVersion exclude("com.typesafe.akka", "akka-actor_2.10")
    exclude("com.typesafe.akka", "akka-remote_2.10") exclude("com.typesafe.akka", "akka-slf4j_2.10")
    excludeAll(ExclusionRule(organization = "org.eclipse.jetty")),
  "org.apache.kafka" % "kafka_2.10" % "0.8.2.2",
  "mysql" % "mysql-connector-java" % "5.1.39",
  "org.apache.zookeeper" % "zookeeper" % "3.4.5" exclude("org.jboss.netty", "netty"),
  "com.yammer.metrics" % "metrics-core" % "2.2.0",
  "org.apache.commons" % "commons-csv" % "1.4",
  "joda-time" % "joda-time" % "2.9.3",
  "org.mongodb" % "casbah-commons_2.10" % "2.8.0",
  "org.mongodb" % "casbah-core_2.10" % "2.8.0",
  "org.mongodb" % "casbah-query_2.10" % "2.8.0",
  "org.mongodb" % "mongo-java-driver" % "2.13.0",
  "org.datanucleus" % "datanucleus-core" % "3.2.10",
  "org.datanucleus" % "datanucleus-rdbms" % "3.2.10",
  "org.datanucleus" % "datanucleus-api-jdo" % "3.2.6",
  "com.typesafe.akka" % "akka-remote_2.10" % "2.3.4",
  "com.typesafe.akka" % "akka-actor_2.10" % "2.3.4",
  "com.typesafe.akka" % "akka-slf4j_2.10" % "2.3.4",
  "com.typesafe.akka" % "akka-stream-experimental_2.10" % "2.0.4",
  "com.typesafe.akka" % "akka-http-core-experimental_2.10" % "2.0.4",
  "com.typesafe.akka" % "akka-http-experimental_2.10" % "2.0.4",
  "com.typesafe.akka" % "akka-http-spray-json-experimental_2.10" % "2.0.4",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.apache.flume" % "flume-ng-core" % "1.6.0",
  "org.apache.flume" % "flume-ng-sdk" % "1.6.0",
  "ch.ethz.ganymed" % "ganymed-ssh2" % "build210",
  "org.kitesdk" % "kite-morphlines-core" % kiteVersion,
  "org.kitesdk" % "kite-morphlines-avro" % kiteVersion,
  "org.kitesdk" % "kite-morphlines-hadoop-core" % kiteVersion excludeAll(ExclusionRule(organization = "org.eclipse.jetty")),
  "org.kitesdk" % "kite-morphlines-json" % kiteVersion,
  "org.kitesdk" % "kite-morphlines-solr-cell" % kiteVersion,
  "org.kitesdk" % "kite-morphlines-solr-core" % kiteVersion
).map(
  _.excludeAll(ExclusionRule(organization="javax.servlet"))
)

//libraryDependencies ++=Seq(
//  "org.scala-lang" % "scala-actors" % "2.10.4"
//)
libraryDependencies ++=Seq(
  "org.quartz-scheduler" % "quartz" % "2.2.2",
  "com.typesafe.akka" % "akka-testkit_2.10" % "2.3.4",
  "org.specs2" %% "specs2" % "2.3.12",
  "junit" % "junit" % "4.7",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
  "org.slf4j" % "slf4j-api" % "1.7.7",
  "org.slf4j" % "slf4j-log4j12" % "1.7.21",
  "log4j" % "log4j" % "1.2.17",
  "com.eed3si9n" % "sbt-assembly_2.8.1" % "sbt0.10.1_0.5"
)

fork in Test := true


