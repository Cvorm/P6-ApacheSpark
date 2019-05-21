import org.apache.spark.serializer.KryoSerializer
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
      .master("local[4]")
      .appName("SimbaExperiment")
      //.config("spark.serializer", classOf[KryoSerializer].getName)
      .getOrCreate()

    import simbaSession.implicits._
    import simbaSession.simbaImplicits._
    val r: Regex = raw"""(-?\d*\.\d*),(-?\d*\.\d*)""".r
    val getPointUDF = udf{ s: mutable.WrappedArray[String] =>  Point(Array(s(0).toDouble, s(1).toDouble)) }
    val getPolygonUDF = udf{ s: mutable.WrappedArray[String] =>
      val i = s.map{x => val tmp = r.findAllIn(x.toString).matchData map(m => Point(Array(m.group(1).toDouble, m.group(2).toDouble)))
        val kk = tmp.toArray
        kk
      }.toArray
      val p = i.map(z => Polygon(z :+ z(0)))
      p
    }
    /* Load data */
    val data = simbaSession.read.json(args(0)).persist()
    /*Run Point Experiment */
    val pointDF = data.select("geometry.coordinates", "geometry.type").filter(entry => entry(1) == "Point").withColumn("coordinates", getPointUDF(col("coordinates"))).persist()
    val knnPointResults = RunKNNPoint(simbaSession, pointDF, args(1).toInt)
    val rangePointResults = RunRangePoint(simbaSession, pointDF)
    val indexPointsResults = RunIndexPoint2(simbaSession, pointDF, args(1).toInt)
    /*Run Polygon Experiment */
    val polyDF = data.select("geometry.coordinates", "geometry.type").filter(x => x(1) == "MultiPolygon" || x(1) == "Polygon").withColumn("coordinates", getPolygonUDF(col("coordinates"))).persist()
    val knnPolygonResults = RunKNNPolygon(simbaSession, polyDF.select("coordinates"), args(1).toInt)
    //RunRangePolygon(simbaSession,  polyDF.select("coordinates"))

    println(knnPointResults, rangePointResults, indexPointsResults, knnPolygonResults)
    /* Close Simba */
    println("POINT DF COUNT: " + pointDF.count())
    println("POLYGON DF COUNT: " + polyDF.count())

    simbaSession.stop()
  }

  def RunKNNPolygon(simba: SimbaSession, df: Dataset[Row], k: Int): String = {
    val t1 = System.currentTimeMillis()
    df.knn("coordinates", Array(15.0, 14.1), k).show(3, false)
    val t2 = System.currentTimeMillis()
    return "RUNTIME POLYGON KNN: " + (t2 - t1) + "\n"
  }

  def RunRangePolygon(simba: SimbaSession, df: Dataset[Row]): String = {
    val t1 = System.currentTimeMillis()
    df.printSchema()
    df.range("coordinates", Array(-100.00,-100.00), Array(100.00, 100.00)).show(5, false)
    val t2 = System.currentTimeMillis()
    return "RUNTIME POLYGON RANGE QUERY: " + (t2 - t1) + "\n"
  }

  def RunKNNPoint(simba: SimbaSession, df: Dataset[Row], k: Int): String = {
    val t1 = System.currentTimeMillis()
    df.knn("coordinates", Array(15.0, 14.1), k).show(5, false)
    val t2 = System.currentTimeMillis()
    return "RUNTIME POINT KNN: " + (t2 - t1) + "\n"
  }

  def RunRangePoint(simba: SimbaSession, df: Dataset[Row]): String = {
    val t1 = System.currentTimeMillis()
    df.range("coordinates", Array(-100.00, -100.00), Array(100.00, 100.00)).show(5, false)
    val t2 = System.currentTimeMillis()
    return "RUNTIME POINT KNN: " + (t2 - t1) + "\n"
  }

  def RunIndexPoint2(simba: SimbaSession, df: Dataset[Row], k: Int): String = {
    val t1 = System.currentTimeMillis()
    val dfindexed = df.index(RTreeType, "indexname", Array("coordinates"))
    val t2 = System.currentTimeMillis()
    //println("RUNTIME POINT INDEX (T-TREE): " + (t2 - t1))
    val t3 = System.currentTimeMillis()
    dfindexed.knn("coordinates", Array(15.0, 14.1), k).show(5, false)
    val t4 = System.currentTimeMillis()
    //println("RUNTIME POINT INDEX (T-TREE) - knn query: " + (t4 - t3))
    val t5 = System.currentTimeMillis()
    dfindexed.range("coordinates", Array(-100.00, -100.00), Array(100.00, 100.00)).show(5, false)
    val t6 = System.currentTimeMillis()
    //println("RUNTIME POINT INDEX (T-TREE) - range query: " + (t6 - t5))
    return "RUNTIME POINT INDEX (R-TREE): " + (t2 - t1) + "\nRUNTIME POINT INDEX (R-TREE) - knn query: " + (t4 - t3) + "\nRUNTIME POINT INDEX (R-TREE) - range query: " + (t6 - t5) + "\n"
  }

}
