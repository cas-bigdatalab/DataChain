name := "DataChain"

version := "1.0"

scalaVersion := "2.10.4"

val hadoopVersion := "2.6.0"

val sparkVersion = "1.6.1"

libraryDependencies ++= Seq(

  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "compile" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile"


)

fork in Test := true
