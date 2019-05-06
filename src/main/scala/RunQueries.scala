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
    spatial.buildIndex(IndexType.RTREE, buildOnSpatialPartitionedRDD)
    print("--Successfully built R-tree--\n")
    return spatial
  }

  def KNNQueryWithRtree(spatialRDD: SpatialRDD[Geometry]): Unit ={
    val geometryFactory = new GeometryFactory()
    val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
    val K = 10 // K Nearest Neighbors
    val usingIndex = true
    val result = KNNQuery.SpatialKnnQuery(spatialRDD, pointObject, K, usingIndex)
    print("--KNN Query result--\n")
    print(result + "\n")
  }

  def RangeQueryWithRtree(spatial: SpatialRDD[Geometry], session: SparkSession): Unit ={
    val rangeQueryWindow = new Envelope(0, 200, 0, 200) //Coordinates for the lower left and upper right point for a BB!
    val considerBoundaryIntersection = false // Only return geometries fully covered by the window
    val usingIndex = true
    var result = RangeQuery.SpatialRangeQuery(spatial, rangeQueryWindow, considerBoundaryIntersection, usingIndex)
    print("--Range Query result--\n")
    print(result.take(10))
//    var lul = Adapter.toDf(result[Geometry], session)
//    lul.show()
//    print(result)
    print(result.count())
  }
}
