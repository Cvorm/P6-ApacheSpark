import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.formatMapper.GeoJsonReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD._
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter
//import org.opengis.geometry.Geometry
import com.vividsolutions.jts.geom.Geometry

class GeoSpark {
  def GeosparkSession() : SparkSession = {
//    val conf = new SparkConf()
//    conf.setAppName("GeoSparkRunnableExample") // Change this to a proper name
//    conf.setMaster("local[*]") // Delete this if run in cluster mode
//    // Enable GeoSpark custom Kryo serializer
//    conf.set("spark.serializer", classOf[KryoSerializer].getName)
//    conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
//    val sc = new SparkContext(conf)
//    return sc
      var sparkSession = SparkSession.builder()
        .master("local[*]") // Delete this if run in cluster mode
        .appName("readTestScala") // Change this to a proper name
        // Enable GeoSpark custom Kryo serializer
        .config("spark.serializer", classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
      return sparkSession


  }
  def GEOJsonFromFileToRDD(filepath: String, session: SparkSession) : SpatialRDD[Geometry]={
    val inputLocation = filepath
//    val data = session.read.json(filepath).drop("features(0)")
//    data.
//    data
//    val newloc = "resources/fix.geojson"
//    data.write.json(newloc)
    val allowTopologyInvalidGeometris = true // Optional
    val skipSyntaxInvalidGeometries = false // Optional
    val spatialRDD = GeoJsonReader.readToGeometryRDD(session.sparkContext, inputLocation, allowTopologyInvalidGeometris, skipSyntaxInvalidGeometries)
//    var spatialDf = Adapter.toDf(spatialRDD, session)
//    spatialDf.printSchema()
//    val counddd = spatialDf.count()
//    print(counddd)
    return spatialRDD

    //SpatialRDD[Geometry]
  }
}
