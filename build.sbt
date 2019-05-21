name := "P6-ApacheSpark"

version := "1.0"

scalaVersion := "2.11.8"
unmanagedBase := baseDirectory.value / "lib"
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
/*libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.2",
  "org.apache.spark" %% "spark-sql" % "2.4.2",
  "org.apache.spark" %% "spark-streaming_2.12" % "2.4.2"
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided"
)*/

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
   "org.apache.hadoop" % "hadoop-client" % "2.1.0"% "provided",
   "com.vividsolutions" % "jts-core" % "1.14.0",
    "org.scalactic" %% "scalactic" % "3.0.1",
    "org.apache.spark" %% "spark-catalyst" % "2.1.0" % "provided"
)
