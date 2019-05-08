import java.sql.Struct

import BasicSpatialOps.Pointy
import com.esotericsoftware.kryo.Kryo
import com.vividsolutions.jts.geom.Geometry
import io.netty.handler.codec.marshalling.CompatibleMarshallingEncoder
import javax.validation.constraints.Digits
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.simba.index.{RTreeType, TreapType}
import org.apache.spark.sql.simba.spatial.{Circle, LineSegment, MBR, Point, Polygon, Shape}
import org.apache.spark.sql.simba.util.KryoShapeSerializer
import org.apache.spark.sql.simba.{Dataset, ShapeSerializer, SimbaSession, spatial}

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
class TestRegistrator
  extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    //val kryo = new Kryo()
    kryo.register(classOf[Shape], new KryoShapeSerializer)
    kryo.register(classOf[Point], new KryoShapeSerializer)
    kryo.register(classOf[MBR], new KryoShapeSerializer)
    kryo.register(classOf[Polygon], new KryoShapeSerializer)
    kryo.register(classOf[Circle], new KryoShapeSerializer)
    kryo.register(classOf[LineSegment], new KryoShapeSerializer)
    kryo.addDefaultSerializer(classOf[Shape], new KryoShapeSerializer)
    kryo.setReferences(false)
  }
}
  object BasicSpatialOps {
    case class Pointy(x: Double, y: Double)
    //case class PointData(x: Double, y: Double, z:Double, other: String)
    case class PointData(x: Double, y: Double)
    case class PointData2(value: Seq[PointData])
    case class Polygony(value: Array[Pointy])

    def main(): Unit = {
      val simbaSession = SimbaSession
        .builder()
        .master("local[4]")
        .appName("SparkSessionForSimba")
        .config("simba.join.partitions", "20")
        .config("spark.serializer", classOf[KryoSerializer].getName)
        //.config()
        //.config("spark.kryo.registrator", classOf[TestRegistrator].getName)
        .getOrCreate()
      //simbaSession.sparkContext.getConf.get("spark.kryo.registrator")
      TestFunction(simbaSession)
      //simbaSession.indexTable("b", RTreeType, "RtreeForData",  Array("coordinates") )
      //runRangeQuery(simbaSession)
      //runKnnQuery(simbaSession)
      //FormatData(simbaSession)
      //runJoinQUery(simbaSession)
      simbaSession.stop()
    }
     def TestFunction(simba: SimbaSession) : Unit = {
      import simba.implicits._
      import simba.simbaImplicits._
       val r: Regex = raw"""(\d*\.\d*),(\d*\.\d*)""".r
       val df2 = simba.read.json("resources/test.json").as[Polygony]//.collect()(0).getInt(0)
      val df = simba.read.json("resources/formattedGeo2.geojson").filter(x=>x(1) == "Polygon").map({
        x=>
        val g = r.findAllIn(x.toString()).matchData map {m => Pointy(m.group(1).toDouble,m.group(2).toDouble)}
        //val pointa = g.toArray[PointData].map(x=> Point(Array(x.x, x.y)))
          val pointArray = g.toArray[Pointy]
          pointArray :+ pointArray(0)
      }).as[Array[Pointy]] //.write.json("resources/succ.geojson") //.show(10, false)
       val p = df2.collect()
       val dsPolygon = (0 until p.length).map({ x =>
         Polygon(p(x).value.map(i=> Point(Array(i.x, i.y))))
       }).toDS()
       val dsPoint = (0 until p.length).map({ x=>
         val tmp = (0 until p(x).value.length).map(y => Point(Array(p(x).value(y).x, p(x).value(y).y)))
         tmp.toArray
       }).flatMap(d=>d).toDS()
       //dsPoint.show(10,false)
       //dsPoint.printSchema()
       dsPolygon.foreach(x=> print(x.getMBR))
       dsPolygon.index(RTreeType, "indexname", Array("value")).show(10,false)
       dsPolygon.knn("value", Array(15.0, 14.1), 3).show(3)
       dsPolygon.range("value",Array(13.7, 50.0),Array(16.0, 52.0)).show(10,false)
       //dsPoint.index(RTreeType, "index", Array("value"))
       //dsPoint.createOrReplaceTempView("b")
       //simba.indexTable("b", RTreeType, "RtreeForData",  Array("value") )
       dsPoint.collect()
    }
    def FormatData(simba: SimbaSession):Unit = {
      import simba.implicits._
      import simba.simbaImplicits._
      import org.apache.spark.sql.functions._
      val r: Regex = raw"""(\d*\.\d*),(\d*\.\d*)""".r //Reg.exp used for getting string X and Y values
      val df = simba.read.json("resources/output.geojson") //Load data
      val len = df.select(explode($"features")).count().toInt -1 //Lenght of DataFrame, used in loop
      val dfPoly = (1 until len).map({ //Maps each feature to a polygon
        x =>
          val ds = df.selectExpr(s"features[$x].geometry.coordinates").map({ //Selects x features coordinates
            y =>
            val g = r.findAllIn(y.toString()) //Finds all Points in row
            g.hasNext
            val points = for (j <- Array.range(0,  r.findAllIn(y.toString()).length , 1)) yield {
              var point = Pointy(g.group(1).toDouble,g.group(2).toDouble)
              g.next()
              point
            }
            points :+ points(0) //Adds first point to end, such that it satisfies polygon criteria
          })
         // val i = ds.index(RTreeType,"test", Array("value"))
          val k = ds.collect.flatMap(x=> x.map(y => Point(Array(y.x, y.y)))) //Collects and flattens dataframe created
          Polygon(k)
      }).toDS()
      dfPoly.printSchema()
      dfPoly.show(10,false)
      //dfPoly.knn("value", Array(15.0, 14.0), 3).show(10,false)
      //return dfPoly
    }
    private def runKnnQuery(simba: SimbaSession): Unit = {
      import org.apache.spark.sql.functions.udf
      import simba.implicits._
      import simba.simbaImplicits._
      import org.apache.spark.sql.functions._


    }
  }


