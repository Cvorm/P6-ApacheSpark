import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.formatMapper.GeoJsonReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialRDD._
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter
//import org.opengis.geometry.Geometry
import com.vividsolutions.jts.geom.Geometry

class RunQueries {
  def KNNQueryLul(spatialRDD: SpatialRDD[Geometry]): Unit ={
    val geometryFactory = new GeometryFactory()
    val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
    val K = 10 // K Nearest Neighbors
    val usingIndex = false
    val result = KNNQuery.SpatialKnnQuery(spatialRDD, pointObject, K, usingIndex)
    print(result)
  }
}
