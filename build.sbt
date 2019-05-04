name := "P6-ApacheSpark"

version := "0.1"

scalaVersion := "2.11.8"
////licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))
//
//
//// https://mvnrepository.com/artifact/org.apache.spark/spark-core
///*libraryDependencies ++= Seq(
//  /*"org.apache.spark" %% "spark-core" % "2.4.2",
//  "org.apache.spark" %% "spark-sql" % "2.4.2",
//  "org.apache.spark" %% "spark-streaming_2.12" % "2.4.2"*/
//  "org.apache.spark" %% "spark-core" % "2.1.0",
//  "org.apache.spark" %% "spark-sql" % "2.1.0",
//  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided"
//)*/
//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
//
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
//
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided"
//
//libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"
//
//libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.1.0"
//
////libraryDependencies +="org.datasyslab" % "geospark" % "1.2.0" % "provided"
////
////libraryDependencies +="org.datasyslab" % "geospark-sql_2.3" % "1.2.0"
////
////libraryDependencies +="org.datasyslab" % "geospark-viz_2.3" % "1.2.0"
//lazy val geoSparkDependencies = Seq( "org.datasyslab" % "geospark" % "1.2.0", "org.datasyslab" % "geospark-sql_2.1" % "1.2.0", "org.datasyslab" % "geospark-viz_2.1" % "1.2.0" )
//
////libraryDependencies += "org.datasyslab" %% "geospark-sql" % "1.2.0"
//
////libraryDependencies += "org.datasyslab" %% "geospark-viz" % "1.2.0"
////libraryDependencies += "com.typesafe" % "config" % "1.3.1"

import sbt.Keys.{libraryDependencies, version}


lazy val root = (project in file(".")).
  settings(
    name := "GeoSparkScalaTemplate",

    version := "0.1.0",

    scalaVersion := "2.11.11",

    organization := "org.datasyslab",

    publishMavenStyle := true
  )

val SparkVersion = "2.1.0"

val SparkCompatibleVersion = "2.3"

val HadoopVersion = "2.7.2"

val GeoSparkVersion = "1.2.0"

val dependencyScope = "compile"

logLevel := Level.Warn


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.spark" %% "spark-sql" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopVersion % dependencyScope,
  "org.apache.hadoop" % "hadoop-common" % HadoopVersion % dependencyScope,
  "org.datasyslab" % "geospark" % GeoSparkVersion,
  "org.datasyslab" % "geospark-sql_".concat(SparkCompatibleVersion) % GeoSparkVersion ,
  "org.datasyslab" % "geospark-viz_".concat(SparkCompatibleVersion) % GeoSparkVersion,
  "com.googlecode.json-simple" % "json-simple" % "1.1.1"
)


resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers +=
  "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"