import java.sql.Struct

import BasicSpatialOps.Pointy
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.simba.index.{RTreeType, TreapType}
import org.apache.spark.sql.simba.spatial.{Point, Polygon, Shape}
import org.apache.spark.sql.simba.{Dataset, SimbaSession, spatial}

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
      val dfPoly = (1 until 10).map({//
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
          val i = ds.index(RTreeType,"test", Array("value"))
          val k = ds.collect.flatMap(x=> x.map(y => Point(Array(y.x, y.y))))
          Polygon(k)
      }).toDS()
      dfPoly.printSchema()
      dfPoly.show(10,false)
      //dfPoly.knn("value", Array(15.0, 14.0), 3).show(10,false)
      return dfPoly
    }
    private def runKnnQuery(simba: SimbaSession): Unit = {
      import org.apache.spark.sql.functions.udf
      import simba.implicits._
      import simba.simbaImplicits._
      import org.apache.spark.sql.functions._


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

    useIndex2(simbaSession)
    FormatData(simbaSession)
    //useIndex1(simbaSession)

    //simbaSession.stop()
  }
  private def FormatData(simba: SimbaSession):Unit = {
    import simba.implicits._
    import simba.simbaImplicits._
    import org.apache.spark.sql.functions._
    org.apache.spark.sql.simba.index.RTree
    val r: Regex = raw"""(\d*\.\d*),(\d*\.\d*)""".r
    val df = simba.read.json("resources/output.geojson")
    val len = df.select(explode($"features")).count().toInt
    val test : Dataset[Array[PointData]] = null
    val dfPoly = (1 until 5).map({ //
      x =>
        val ds = df.selectExpr(s"features[$x].geometry.coordinates").map({
          y =>
            val g = r.findAllIn(y.toString())
            g.hasNext
            val points = for (j <- Array.range(0, r.findAllIn(y.toString()).length, 1)) yield {
              var point = Pointy(g.group(1).toDouble, g.group(2).toDouble)
              g.next()
              point
            }
            points :+ points(0)
        })
        ds.printSchema()
        //val i = ds.toDF().index(RTreeType,"test", Array("coordinates"))
        //test = ds.flatMap(x => x.map(y => Seq(PointData(y.x, y.y, 1.1, "rtes"))))
        val k:Seq[PointData] = ds.collect.flatMap(x => x.map(y => PointData(y.x, y.y, 0, "1")))
        k
    }).toDS()
    dfPoly.printSchema()

    dfPoly.index(RTreeType,"what", Array("value"))
    //simba.indexTable("b", RTreeType, "RtreeForData",  Array("$value", "value") )
    dfPoly.printSchema()

  }
  private def buildIndex(simba: SimbaSession): Unit = {
    import simba.implicits._
    val df = FormatData(simba)
    //df.createOrReplaceTempView("a")

    simba.indexTable("a", RTreeType, "testqtree",  Array("value(1)", "value(2)") )

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
    println("stop")

  }

  private def useIndex2(simba: SimbaSession): Unit = {
    import simba.implicits._
    Polygon()
    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDF()
    val dp = Seq(Point(Array(1.0, 1.0)), Point(Array(4.0, 1.0)), Point(Array(3.0, 1.2)), Point(Array(1.0, 4.0)), Point(Array(2.0, 6.0))).toDF()
    val dp = Seq(Polygon(Array(Point(Array(1.0, 1.0)), Point(Array(4.0, 1.0))), Polygon(Point(Array(3.0, 1.2))), Point(Array(1.0, 4.0)) )
    dp.createOrReplaceTempView("b")
    dp.printSchema()

    simba.indexTable("b", RTreeType, "RtreeForData",  Array("value") )

    simba.showIndex("b")

    simba.sql("SELECT * FROM b where b.x >1 and b.y<=2").show(5)
    println("stop")
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