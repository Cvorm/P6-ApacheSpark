import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.formatMapper.GeoJsonReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.{KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD._
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter
//import org.opengis.geometry.Geometry
import com.vividsolutions.jts.geom.Geometry


class RunQueries {

  def BuildRtreeIndex(spatial: SpatialRDD[Geometry]): SpatialRDD[Geometry] ={
    val buildOnSpatialPartitionedRDD = false // Set to TRUE only if run join query
    val t1 = System.currentTimeMillis()

    spatial.buildIndex(IndexType.RTREE, buildOnSpatialPartitionedRDD)

    val t2 = System.currentTimeMillis()
    println("RUNTIME BUILD R-TREE: " + (t2 - t1))
    print("--Successfully built R-tree--\n")
    return spatial
  }

  def KNNQueryWithRtree(spatialRDD: SpatialRDD[Geometry],session: SparkSession, neighbors: Integer): Unit ={
    val t1 = System.currentTimeMillis()

    val geometryFactory = new GeometryFactory()
    val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
    val K = neighbors // K Nearest Neighbors
    val usingIndex = true
    val result = KNNQuery.SpatialKnnQuery(spatialRDD, pointObject, K, usingIndex)

    val t2 = System.currentTimeMillis()

    println("RUNTIME KNN-QUERY: " + (t2 - t1))
    print("--KNN Query result--\n")
    print(result + "\n")
  }

  def RangeQueryWithRtree(spatial: SpatialRDD[Geometry], session: SparkSession): Unit ={
    val t1 = System.currentTimeMillis()

    val rangeQueryWindow = new Envelope(-100, 100, -100, 100) //Coordinates for the lower left and upper right point for a BB!
    val considerBoundaryIntersection = false // Only return geometries fully covered by the window
    val usingIndex = true
    var result = RangeQuery.SpatialRangeQuery(spatial, rangeQueryWindow, considerBoundaryIntersection, usingIndex)

    val t2 = System.currentTimeMillis()

    println("RUNTIME RANGE-QUERY: " + (t2 - t1))
    print("--Range Query result--\n")
    print(result.take(5))
//    var lul = Adapter.toDf(result[Geometry], session)
//    lul.show()
//    print(result)
//    print(result.count())
  }
}
