import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.simba.{Dataset, ShapeSerializer}
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.functions._
import scala.collection.mutable

object Simba {
  import org.apache.spark.sql.simba.spatial.{Point, Polygon}
  import org.apache.spark.sql.simba.SimbaSession
  import scala.util.matching.Regex

  def main(args: Array[String]): Unit = {

    val simbaSession = SimbaSession
      .builder()
      //.master("local[4]")
      .appName("SimbaExperiment")
      //.config("spark.serializer", classOf[KryoSerializer].getName)
      .getOrCreate()

    simbaSession.conf.set("spark.sql.codegen.wholeStage", false)
    import simbaSession.implicits._
    import simbaSession.simbaImplicits._
    val r: Regex = raw"""(-?\d*\.\d*),(-?\d*\.\d*)""".r
    val getPointUDF = udf{ s: mutable.WrappedArray[String] =>  Point(Array(s(0).toDouble, s(1).toDouble)) }
    val getPolygonUDF = udf{ s: mutable.WrappedArray[String] =>
      val points = s.map{x => val tmp = r.findAllIn(x.toString).matchData map(m => Point(Array(m.group(1).toDouble, m.group(2).toDouble)))
        val pointArray = tmp.toArray
        pointArray
      }.toArray
      val p = points.map(ap => Polygon(ap :+ ap(0))) //Adds the first point of polygon to the end
      p
    }
    val getPolygonDoubleUDF = udf{wa: mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]]] =>
      wa.map { x =>
        val points = x.map { y =>
          val p = y.map(z => Point(Array(z(0), z(1))))
          val pointArray = p.toArray
          pointArray
        }
      points.map(p => Polygon(p))
      }
    }
    val getAllPointsUDF = udf{ s: mutable.WrappedArray[String] =>
      val points = s.flatMap{x => val tmp = r.findAllIn(x.toString).matchData map(m => Array(m.group(1).toDouble, m.group(2).toDouble))
        val pointArray = tmp.map( x=> Point(x))
        //pointArray.foreach(println)
        pointArray
      }
      val k = points.array.map(z=>z)
      k
    }
    val getPointsDoubleUDF = udf{d: mutable.WrappedArray[Double] =>  Point(Array(d(0), d(1))) }
    /* Load data */
    val data = simbaSession.read.json(args(0)).select("geometry").persist()

    /*Run Point Experiment */
    val pointDF = data.select("geometry.coordinates", "geometry.type").filter(entry => entry(1) == "Point").withColumn("coordinates", getPointsDoubleUDF(col("coordinates"))).persist()
    pointDF.show(10,false)
    val randomPointFromSet = pointDF.select("coordinates").as[Point].take(1).flatMap(x=> x.coord)
    val rangeQueryWindow = Array(randomPointFromSet, Array(randomPointFromSet(0) + args(3).toDouble, randomPointFromSet(1) + args(3).toDouble))

    val r1 = RunKNNPoint(simbaSession, pointDF.select("coordinates"), args(2).toInt, randomPointFromSet) //randomPointFromSet
    val r2 = RunRangePoint(simbaSession, pointDF.select("coordinates"), rangeQueryWindow) //rangeQueryWindow
    val r3 = RunIndexPoint2(simbaSession, pointDF.select("coordinates"), args(2).toInt,randomPointFromSet, rangeQueryWindow)
    /*Run Polygon Experiment */
    data.unpersist()
    pointDF.unpersist()

    val polydata = simbaSession.read.json(args(1)).select("geometry").persist()
    val polyDF = polydata.select("geometry.coordinates", "geometry.type").filter(x => x(1) == "MultiPolygon" || x(1) == "Polygon").withColumn("coordinates", getPolygonDoubleUDF(col("coordinates"))).persist()
    //polyDF.show(10,false)
    val r4 = RunKNNPolygon(simbaSession, polyDF.select("coordinates"), args(2).toInt, randomPointFromSet)
    //RunRangePolygon(simbaSession,  polyDF.select("coordinates")) NOT SUPPORTED
    println(r1, r2, r3, r4, "POINT DF COUNT: " + pointDF.count() + "\nPOLYGON DF COUNT: " + polyDF.count() + "\nPoint: " + randomPointFromSet(0), randomPointFromSet(1) + "\nRange window: " + rangeQueryWindow(0)(0),rangeQueryWindow(0)(1),rangeQueryWindow(1)(0),rangeQueryWindow(1)(1))

    /* Close Simba */
    simbaSession.stop()
  }

  def RunKNNPolygon(simba: SimbaSession, df: Dataset[Row], k: Int,p: Array[Double] ): String = {
    val t1 = System.currentTimeMillis()
    df.knn("coordinates", p, k).show(3, false)
    val t2 = System.currentTimeMillis()
    return "RUNTIME POLYGON KNN: " + (t2 - t1) + "\n"
  }

  def RunRangePolygon(simba: SimbaSession, df: Dataset[Row]): String = {
    val t1 = System.currentTimeMillis()
    df.range("coordinates", Array(-100.00,-100.00), Array(100.00, 100.00)).show(5, false)
    val t2 = System.currentTimeMillis()
    return "RUNTIME POLYGON RANGE QUERY: " + (t2 - t1) + "\n"
  }

  def RunKNNPoint(simba: SimbaSession, df: Dataset[Row], k: Int, p: Array[Double]): String = {
    val t1 = System.currentTimeMillis()
    df.knn("coordinates",p, k).show(5, false)
    val t2 = System.currentTimeMillis()
    return "RUNTIME POINT KNN: " + (t2 - t1) + "\n"
  }

  def RunRangePoint(simba: SimbaSession, df: Dataset[Row],w: Array[Array[Double]]): String = {
    val t1 = System.currentTimeMillis()
    df.range("coordinates", w(0), w(1)).show(5, false)
    val t2 = System.currentTimeMillis()
    return "RUNTIME POINT RANGE QUERY: " + (t2 - t1) + "\n"
  }

  def RunIndexPoint2(simba: SimbaSession, df: Dataset[Row], k: Int, p: Array[Double], w: Array[Array[Double]]): String = {
    val t1 = System.currentTimeMillis()
    val dfindexed = df.index(RTreeType, "indexname", Array("coordinates"))
    val t2 = System.currentTimeMillis()
    //println("RUNTIME POINT INDEX (T-TREE): " + (t2 - t1))
    val t3 = System.currentTimeMillis()
    dfindexed.knn("coordinates", p, k).show(5, false)
    val t4 = System.currentTimeMillis()
    //println("RUNTIME POINT INDEX (T-TREE) - knn query: " + (t4 - t3))
    val t5 = System.currentTimeMillis()
    dfindexed.range("coordinates", w(0), w(1)).show(5, false)
    val t6 = System.currentTimeMillis()
    //println("RUNTIME POINT INDEX (T-TREE) - range query: " + (t6 - t5))
    return "RUNTIME POINT INDEX (R-TREE): " + (t2 - t1) + "\nRUNTIME POINT INDEX (R-TREE) - knn query: " + (t4 - t3) + "\nRUNTIME POINT INDEX (R-TREE) - range query: " + (t6 - t5) + "\n"
  }

}
