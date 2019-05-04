import java.sql.Struct

import org.apache.spark.sql.simba.index.{RTreeType, TreapType}
import org.apache.spark.sql.simba.spatial.{Point, Polygon, Shape}
import org.apache.spark.sql.simba.{Dataset, SimbaSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
object Simba {
  case class Pointx(d: Double, d1: Double)
  //case class PointData(x: Double, y:Double, z:Double, other:String)
  case class PointData(p: Point, payload: Int)
  case class Feature(geometry : Array[Array[Array[Double]]])
  def Build(): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("SIMBA")
      .config("simba.index.partitions", "64")
      .getOrCreate()
    /*
    buildIndex(simbaSession)
    useIndex1(simbaSession)
    useIndex2(simbaSession)
    runRangeQuery(simbaSession)
    runKnnQuery(simbaSession)
  */

    //toSchemaRDD.write.json("types")

   /* var result = input.flatMap(record =>
      try {
        people.add(mapper.readValue(line, Person.
        class
        ) ) lassOf[]
      })*/
      //simbaSession.stop()
      //JsonReader
  }
}
object SpatialClassInference {
  case class PointData(p: Point, payload: Int)

  def main() = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("SpatialClassInference")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    import simbaSession.implicits._
    import simbaSession.simbaImplicits._
    val ps = (0 until 10000).map(x => PointData(Point(Array(x.toDouble, x.toDouble)), x + 1)).toDS
    ps.knn("p", Array(1.0, 1.0), 4).show()
    ps.printSchema()
    ps.range("p", Array(1.0, 2.0), Array(4.0, 5.0)).show(

    )
  }
}
  object BasicSpatialOps {
    case class Pointy(x: Double, y: Double)
    //case class PointData(x: Double, y: Double, z:Double, other: String)
    case class PointData(p: Point, payload: Int)
    case class Polygony(polygon: Array[Point])

    def main(): Unit = {

      val simbaSession = SimbaSession
        .builder()
        .master("local[4]")
        .appName("SparkSessionForSimba")
        .config("simba.join.partitions", "20")
        .getOrCreate()

      //runRangeQuery(simbaSession)
      //runKnnQuery(simbaSession)
      FormatData(simbaSession)
      //runJoinQUery(simbaSession)
      simbaSession.stop()
    }
    private def FormatData(simba: SimbaSession):Unit = {
      import simba.implicits._
      import simba.simbaImplicits._
      import org.apache.spark.sql.functions._
      val r: Regex = raw"""(\d*\.\d*),(\d*\.\d*)""".r
      val df = simba.read.json("resources/output.geojson")
      val len = df.select(explode($"features")).count().toInt
      val dfPoly = (1 until 50).map({//
        x =>
          val ds = df.selectExpr(s"features[$x].geometry.coordinates").map({
            y =>
            val g = r.findAllIn(y.toString())
            g.hasNext
            val points = for (j <- Array.range(0,  r.findAllIn(y.toString()).length , 1)) yield {
              var point = Pointy(g.group(1).toDouble,g.group(2).toDouble)
              g.next()
              point
            }
            points :+ points(0)
          })
          val k = ds.collect.flatMap(x=> x.map(y => Point(Array(y.x, y.y))))
          Polygon(k)
      }).toDS()
      dfPoly.printSchema()
      dfPoly.show(10,false)
      dfPoly.knn("value", Array(15.0, 14.0), 3).show(10,false)
    }
    private def runKnnQuery(simba: SimbaSession): Unit = {
      import org.apache.spark.sql.functions.udf
      import simba.implicits._
      import simba.simbaImplicits._
      import org.apache.spark.sql.functions._
      //simba.read.option("multiLine", true)
      val df = simba.read.json("resources/output.geojson")
      //val ds: Dataset[Worm] = df.select("features.geometry.coordinates", "features.geometry.type").as[Worm]
      val pattern: Regex = raw"""(\d*\.\d*),(\d*\.\d*)""".r
      //val filted = df.selectExpr("features[11].geometry.coordinates[0]").foreach(y => pattern.findAllIn(y.toString()).matchData foreach(m => Point(m.group(1).toDouble, m.group(2).toDouble)))//)foreach(x=> println(x)) //.map(r => Work(r.toString().split(",")))
      //val filted2 = df.selectExpr("features[11].geometry.coordinates[0]").show(10,false)
      //val whatev = df.select("features").foreach( x=> for(i <- 1 to 3 ){println(x.[i])})
      val getPoints = for (x <- 0 to 10) {
        df.selectExpr(s"features[$x].geometry.coordinates[0]").map({ //.selectExpr("features[11].geometry.coordinates[0]")
          str =>
            val reg = raw"""(\d*\.\d*),(\d*\.\d*)""".r
            val gro = reg.findAllIn(str.toString())
            gro.hasNext
            val suck = for (i <- Array.range(0, reg.findAllIn(str.toString()).length, 1)) yield {
              //var p = PointData(Point(Array(gro.group(1).toDouble, gro.group(2).toDouble)), i +1)
              var p = Point(Array(gro.group(1).toDouble, gro.group(2).toDouble))
              gro.next()
              p
            }
            suck

        }).withColumnRenamed("value", "points").as[Polygon]
      }
      //val su = getPoints.flatMap(y => y)
      //getPoints.knn(Array("points"), Array(14.00, 14.00), 1)
     //getPoints.select("coordinates").knn(Array("x","y"), Array(14.00, 14.00), 3)
      //getPoints.printSchema()
      //getPoints.show(10, false)
      //getPoints.collect().foreach(println)

      //ds.limit(10).select("coordinates").take(10).foreach(println)
      df.show(10)

    }
  }
object IndexExample {
  case class PointData(x: Double, y: Double, z: Double, other: String)

  def main(): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("IndexExample")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    buildIndex(simbaSession)
    //useIndex1(simbaSession)
   // useIndex2(simbaSession)
    //simbaSession.stop()
  }

  private def buildIndex(simba: SimbaSession): Unit = {
    import simba.implicits._
    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS
    val DF = simba.read.json( "resources/output.geojson")
    DF.createOrReplaceTempView("a")

    simba.indexTable("a", RTreeType, "testqtree",  Array("x", "y") )

    simba.showIndex("a")
  }

  private def useIndex1(simba: SimbaSession): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._
    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDF()

    datapoints.createOrReplaceTempView("b")

    simba.indexTable("b", RTreeType, "RtreeForData",  Array("x", "y") )

    simba.showIndex("b")

    val res = simba.sql("SELECT * FROM b")
    res.knn(Array("x", "y"),Array(1.0, 1.0),4).show(4)

  }

  private def useIndex2(simba: SimbaSession): Unit = {
    import simba.implicits._
    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDF()

    datapoints.createOrReplaceTempView("b")

    simba.indexTable("b", RTreeType, "RtreeForData",  Array("x", "y") )

    simba.showIndex("b")

    simba.sql("SELECT * FROM b where b.x >1 and b.y<=2").show(5)

  }

  private def useIndex3(simba: SimbaSession): Unit = {
    import simba.implicits._
    val datapoints = Seq(PointData(0.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS()

    import simba.simbaImplicits._

    datapoints.index(TreapType, "indexForOneTable",  Array("x"))

    datapoints.range(Array("x"),Array(1.0),Array(2.0)).show(4)
  }
}