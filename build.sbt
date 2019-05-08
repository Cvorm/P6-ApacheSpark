name := "P6-ApacheSpark"

version := "0.1"

scalaVersion := "2.11.8"
//licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
/*libraryDependencies ++= Seq(
  /*"org.apache.spark" %% "spark-core" % "2.4.2",
  "org.apache.spark" %% "spark-sql" % "2.4.2",
  "org.apache.spark" %% "spark-streaming_2.12" % "2.4.2"*/
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided"
)*/
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.1.0"
libraryDependencies += "com.googlecode.json-simple" % "json-simple" % "1.1.1"
//libraryDependencies += "com.typesafe" % "config" % "1.3.1"