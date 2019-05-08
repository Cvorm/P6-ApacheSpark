//import org.datasyslab.geosparksql.utils.Adapter
//import dbis.stark._
//import org.apache.spark.SpatialRDD._
//import org.datasyslab.geospark.spatialRDD.SpatialRDD
//import com.vividsolutions.jts.geom.Geometry
//import dbis.stark.spatial.partitioner.{BSPartitioner, SpatialGridPartitioner}
//import org.apache.spark.sql.SparkSession
//import org.apache.spark
//
//import scala.reflect.ClassTag
//
//class Stark {
////  import org.apache.log4j.Level
////  import org.apache.log4j.Logger
////  import org.apache.spark.SparkConf
////  import org.apache.spark._
////  import dbis.stark._
////  import org.apache.spark.SpatialRDD._
////
////  val conf = new SparkConf()
////  conf.setAppName("GeoSparkRunnableExample") // Change this to a proper name
////  conf.setMaster("local[*]")
////
////
//
//
////  def LoadDataStark(spatialRDD: SpatialRDD[Geometry],session: SparkSession): Unit ={
////    import session.implicits._
////    var spatialDf = Adapter.toDf(spatialRDD, session)
//////    var starkSpatial = spatialDf.map(arr=>(STObject(arr(0)))
//////    var starkSpatial = spatialDf.map(line =>line.asInstanceOf[STObject])
////    var starkSpatial2 = spatialDf.select("geometry").map(line2 =>(line2.asInstanceOf[STObject]))
////    val gridPartitioner = new BSPartitioner[](spatialRDD, 0.5, 1000)
////
//////    return starkSpatial2
////    print("XD")
////  }
//  def LoadDataStark(filepath: String, session: SparkSession): Unit ={
//  import  session.implicits._
//  val starkData = session.read.textFile(filepath)
//  starkData.map(line => line.split("},"))
//           .map(arr => (STObject(arr(0))))
//  val gridPartitioner = new SpatialGridPartitioner(starkData, 10)
//}
//}
