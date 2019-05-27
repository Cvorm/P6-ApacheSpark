import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, Point}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.formatMapper.{FormatMapper, GeoJsonReader}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.{KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD._
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter
//import org.opengis.geometry.Geometry
import com.vividsolutions.jts.geom.Geometry
import scala.collection.mutable.ListBuffer

class RunQueries {

  var runtimes = Array[Long](1,2,3,4,5)

  def getRuntimes(): Array[Long] ={
    return runtimes
  }

  def BuildRtreeIndex(spatial: SpatialRDD[Geometry]): SpatialRDD[Geometry] ={
    val buildOnSpatialPartitionedRDD = false // Set to TRUE only if run join query
    val t1 = System.currentTimeMillis()

    spatial.buildIndex(IndexType.RTREE, buildOnSpatialPartitionedRDD)
    val t2 = System.currentTimeMillis()
    var indexTime = (t2 - t1)
    runtimes(0) = indexTime
    println("RUNTIME BUILD R-TREE: " + indexTime)
    print("--Successfully built R-tree--\n")
    return spatial
  }

  def KNNQueryWithRtree(spatialRDD: SpatialRDD[Geometry],session: SparkSession, neighbors: Integer, x1: Double, y1: Double): Unit ={
    val t1 = System.currentTimeMillis()

    val geometryFactory = new GeometryFactory()
    val pointObject = geometryFactory.createPoint(new Coordinate(x1, y1))
    val K = neighbors // K Nearest Neighbors
    val usingIndex = true
    val result = KNNQuery.SpatialKnnQuery(spatialRDD, pointObject, K, usingIndex)

    val t2 = System.currentTimeMillis()

    var KNNRunTime = (t2 - t1)

    runtimes(1) = KNNRunTime
    println("RUNTIME KNN-QUERY: " + KNNRunTime)
    print("--KNN Query result--\n")
    print(result + "\n")
  }

  def KNNQueryWITHOUTIndex(spatialRDD: SpatialRDD[Geometry],session: SparkSession, neighbors: Integer, x1: Double, y1: Double): Unit ={
    val t1 = System.currentTimeMillis()

    val geometryFactory = new GeometryFactory()
    val pointObject = geometryFactory.createPoint(new Coordinate(x1, y1))
    val K = neighbors // K Nearest Neighbors
    val usingIndex = false
    val result = KNNQuery.SpatialKnnQuery(spatialRDD, pointObject, K, usingIndex)


    val t2 = System.currentTimeMillis()

    var KNNRunTime = (t2 - t1)
    runtimes(2) = KNNRunTime
    println("RUNTIME KNN-QUERY: " + KNNRunTime)
    print("--KNN Query result--\n")
    print(result + "\n")
  }

  def RangeQueryWithRtree(spatial: SpatialRDD[Geometry], session: SparkSession, x1: Double, y1: Double, x2: Double, y2: Double): Unit ={
    val t1 = System.currentTimeMillis()

    val rangeQueryWindow = new Envelope(x1, x2, y1, y2) //Coordinates for the lower left and upper right point for a BB!
    val considerBoundaryIntersection = false // Only return geometries fully covered by the window
    val usingIndex = true
    var result = RangeQuery.SpatialRangeQuery(spatial, rangeQueryWindow, considerBoundaryIntersection, usingIndex)

    val t2 = System.currentTimeMillis()

    var RangeRunTime = (t2 - t1)
    runtimes(3) = RangeRunTime
    println("RUNTIME RANGE-QUERY: " + RangeRunTime)
    print("--Range Query result--\n")
    print(result.take(5))
    print("\n")
//    var lul = Adapter.toDf(result[Geometry], session)
//    lul.show()
//    print(result)
//    print(result.count())
  }

  def RangeQueryWITHOUTIndex(spatial: SpatialRDD[Geometry], session: SparkSession, x1: Double, y1: Double, x2: Double, y2: Double): Unit ={
    val t1 = System.currentTimeMillis()

//    val rangeQueryWindow = new Envelope(-100, 100, -100, 100) //Coordinates for the lower left and upper right point for a BB!
    val rangeQueryWindow = new Envelope(x1, x2, y1, y2)
    val considerBoundaryIntersection = false // Only return geometries fully covered by the window
    val usingIndex = false
    var result = RangeQuery.SpatialRangeQuery(spatial, rangeQueryWindow, considerBoundaryIntersection, usingIndex)

    val t2 = System.currentTimeMillis()

    var RangeRunTime = (t2 - t1)
    runtimes(4) = RangeRunTime
    println("RUNTIME RANGE-QUERY: " + RangeRunTime)
    print("--Range Query result--\n")
    print(result.take(5))
    print("\n")
    //    var lul = Adapter.toDf(result[Geometry], session)
    //    lul.show()
    //    print(result)
    //    print(result.count())
  }

//  def KNNwithindex(spatialRDD: SpatialRDD[Geometry], session: SparkSession, neighbors: Int): Unit ={
//    val buildOnSpatialPartitionedRDD = false
//    spatialRDD.buildIndex(IndexType.RTREE, buildOnSpatialPartitionedRDD)
//
//    val geometryFactory = new GeometryFactory()
//    val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
//    val K = neighbors // K Nearest Neighbors
//    val usingIndex = true
//    val result = KNNQuery.SpatialKnnQuery(spatialRDD, pointObject, K, usingIndex)
//  }
////}
//    val points = spatialRDD.rawSpatialRDD.rdd.filter(x => x.getGeometryType == "Point").
//    points.foreach(println)
////    val format = new FormatMapper[Geometry]()
////    val lul = new PointRDD(points)
//    val gottem = GeoJsonReader.readToGeometryRDD(points)
//    val buildOnSpatialPartitionedRDD = false
//    gottem.buildIndex(IndexType.RTREE,buildOnSpatialPartitionedRDD)
////    rdd.rawSpatialRDD.rdd.foreach(println)
//
////    rdd.buildIndex(IndexType.RTREE, buildOnSpatialPartitionedRDD)
////    rdd.indexedRawRDD.persist()
//
//    val geometryFactory = new GeometryFactory()
//    val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
//    val K = neighbors // K Nearest Neighbors
//    val usingIndex = true
//    val result = KNNQuery.SpatialKnnQuery(gottem, pointObject, K, usingIndex)
//  }
}
