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

      var sparkSession = SparkSession.builder()
//        .master("local[*]") // Delete this if run in cluster mode

        .appName("readTestScala") // Change this to a proper name
        // Enable GeoSpark custom Kryo serializer
        .config("spark.serializer", classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
      return sparkSession


  }
  def GEOJsonFromFileToRDD(filepath: String, session: SparkSession) : SpatialRDD[Geometry]={
    val inputLocation = filepath
//    val df = session.read.parquet("resources/relation.parquet")
////    df.show(10)
//    df.printSchema()
//    df.select("members").collect().foreach(println)
//    df.write.parquet("resources/OSM.parquet")
//    val data = session.read.json(filepath).drop("features(0)")
//    data.
//    data
//    val newloc = "resources/fix.geojson"
//    data.write.json(newloc)
    val allowTopologyInvalidGeometris = true // Optional
    val skipSyntaxInvalidGeometries = false // Optional
    val spatialRDD = GeoJsonReader.readToGeometryRDD(session.sparkContext, inputLocation, allowTopologyInvalidGeometris, skipSyntaxInvalidGeometries) //Is on  the same format as a WKT string: POLYGON[...], ... , POLYGON[...]
//    println(spatialRDD)


//    var spatialDf = Adapter.toDf(spatialRDD, session)
//    spatialDf.show(10)

//    val counddd = spatialDf.count()
//    print(counddd)
    return spatialRDD

    //SpatialRDD[Geometry]
  }
}
