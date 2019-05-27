import java.lang

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.geosparksql._
import org.apache.spark.sql.geosparksql.expressions.{ST_GeomFromGeoJSON, ST_GeomFromWKT}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GeometryType, IndexType}
import org.datasyslab.geospark.formatMapper.{FormatMapper, GeoJsonReader, WktReader}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileRDD
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.PolygonParser
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialRDD._
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

import scala.collection.mutable.WrappedArray
//import org.opengis.geometry.Geometry
import com.vividsolutions.jts.geom.Geometry
import scala.util.matching.Regex
import org.apache.spark.sql.functions._


class GeoSpark {
    var numElements = 0.0

    def GetElements(): Double ={
      return numElements
    }
//  def ToPoints(spatialRDD: SpatialRDD[Geometry]): SpatialRDD[Geometry] ={
//    val points = spatialRDD.rawSpatialRDD.rdd.filter(x => x.getGeometryType == "Point").persist()
//    val rdd = GeoJsonReader.readToGeometryRDD(points)
//
//    return rdd
//  }
//
//  def ToPolygons(spatialRDD: SpatialRDD[Geometry]): SpatialRDD[Geometry] ={
//    val polygons = spatialRDD.rawSpatialRDD.rdd.filter(x => x.getGeometryType == "Polygon")
//    val rdd = GeoJsonReader.readToGeometryRDD(polygons)
//
//    return rdd
//  }
  def GeosparkSession() : SparkSession = {

      var sparkSession = SparkSession.builder()
        .master("local[*]") // Delete this if run in cluster mode

        .appName("readTestScala") // Change this to a proper name
        // Enable GeoSpark custom Kryo serializer
        .config("spark.serializer", classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()

//      GeoSparkSQLRegistrator.registerAll(sparkSession)
      return sparkSession


  }
//  def pointsRDD(filepath: String, session: SparkSession): PointRDD[Geometry] ={
//    val inputLocation = filepath
//    val allowTopologyInvalidGeometris = true // Optional
//    val skipSyntaxInvalidGeometries = false // Optional
//    val points = GeoJsonReader.readToGeometryRDD(session.sparkContext, inputLocation, allowTopologyInvalidGeometris, skipSyntaxInvalidGeometries)
//    return points
//  }
//  def GEOJsonPointsAndPoly(filepath: String, session: SparkSession): Unit ={
//  val inputLocation = filepath
//  val allowTopologyInvalidGeometris = true // Optional
//  val skipSyntaxInvalidGeometries = false // Optional
//  val spatialRDD = GeoJsonReader.readToGeometryRDD(session.sparkContext, inputLocation, allowTopologyInvalidGeometris, skipSyntaxInvalidGeometries)
//  val points = spatialRDD.rawSpatialRDD.rdd.filter(x => x.getGeometryType == "Point")
//  val polygons = spatialRDD.rawSpatialRDD.rdd.filter(x => x.getGeometryType == "Point")
//}
  def GEOJsonFromFileToRDD(filepath: String, session: SparkSession) : SpatialRDD[Geometry] ={

  val inputLocation = filepath
  val allowTopologyInvalidGeometris = true // Optional
  val skipSyntaxInvalidGeometries = false // Optional
  val spatialRDD = GeoJsonReader.readToGeometryRDD(session.sparkContext, inputLocation, allowTopologyInvalidGeometris, skipSyntaxInvalidGeometries)
  numElements = spatialRDD.rawSpatialRDD.count().toDouble
  return  spatialRDD

//  import session.implicits._
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
//    val inputLocation = filepath
//    val allowTopologyInvalidGeometris = true // Optional
//    val skipSyntaxInvalidGeometries = false // Optional
//    val spatialRDD = GeoJsonReader.readToGeometryRDD(session.sparkContext, inputLocation, allowTopologyInvalidGeometris, skipSyntaxInvalidGeometries)//Is on  the same format as a WKT string: POLYGON[...], ... , POLYGON[...]
//    var df = Adapter.toDf(spatialRDD, session)
//    df.printSchema()
//    df.createOrReplaceTempView("rawSpatialDf")

//    rawspatialdf.show()
//    var df = session.read.format("json").option("header", "false").load(filepath).select("geometry")
//
//      val Points = df.select("geometry").filter(col("geometry.type") === "Point").persist()
//      val Polygons = df.select("geometry").filter(col("geometry.type") === "Polygon" || col("geometry.type") === "MultiPolygon").persist()
//      print(Points.schema.size)
//      Points.printSchema()
//      Points.show(5)
//      val gottem = Points.select("geometry.type")
//      val bitchass = Points.select(concat_ws(",", col("geometry.coordinates")))
//      val bitchass = Points.withColumn("nice",concat_ws(",",  col("geometry.coordinates")))
//  val thegoodone = Points.select("geometry.coordinates")
//val mkString = udf{(arrayCol: Array[String])=>arrayCol.asInstanceOf[WrappedArray[String]].toArray.mkString(",")}
//  val mkString = udf((arrayCol: WrappedArray[String]) => "POINT" + " " + "(" + arrayCol.mkString(",") + ")")
//val dfgood = Points.select("geometry.coordinates").withColumn("array",mkString(col("coordinates"))).persist()
// val dfj = dfgood.select("array").persist()
//  dfj.createOrReplaceTempView("gg")
//  dfj.foreach(x=>println(x))

//  dfgood.printSchema()
//  dfgood.show()
//  dfgood.createOrReplaceTempView("nice")
//  dfgood.foreach(x=>println(x))

//          val nigga = bitchass.map(x=>("(" + x + ")"))
//          nigga.printSchema()
//          nigga.show()
//      val lololol = (arrayCol: Seq[String]) => Poi
//      val loool = bitchass.select("nice").map(x=>("POINT" + " " + "(" + x + ")" )).toDF("coordinates")
//      loool.createOrReplaceTempView("fedt")
//      bitchass.createOrReplaceTempView("lulz")
//      val looool = Points.select("geometry.coordinates").
//      bitchass.printSchema()
//      bitchass.show()
//      loool.printSchema()
//      loool.show()
//      val gottem2 = Points.select("geometry.coordinates").map(x=>("POINT" + x.toString())).toDF("coordinates")
//      val gottem2 = Points.select("geometry.coordinates").map(x=>("POINT" + " " + (x.getAs[WrappedArray[String]](0)).toArray)).toDF("coordinates")
//      gottem2.createOrReplaceTempView("nice")
//
//      gottem.printSchema()
//      gottem.show()
//      gottem2.printSchema()
//      gottem2.show()
//      gottem2.foreach(x=>println(x))


//
//      var spatialDf = session.sql("""
//                                       | SELECT ST_GeomFromWKT(array) AS shape
//                                       | FROM gg
//                                     """.stripMargin).persist()
//    spatialDf.createOrReplaceTempView("spatialdf")
//    spatialDf.show()
//    spatialDf.printSchema()
////      val gottem3 = Points.withColumn("bitch", concat(gottem.col("type"), lit(" "), gottem2.col("coordinates") ))
////      val gottem3 = gottem2.select(concat(gottem2.col("coordinates"), lit(" "), gottem.col("type")))
////      val Points2 = Points.withColumn("joined", concat(gottem("type"), lit(" "), gottem2("coordinates")))
////      val gottem3 = Points.map(x => concat(gottem.col("type")(x), lit(" "), gottem2.col("coordinates")(x)))
//
//      val lulul = Points.select(col("geometry.type"), lit(" "), col("geometry.coordinates"))
//      val xd = lulul.withColumn("merged", struct(lulul("type"), lulul("coordinates")))
////  lulul.printSchema()
////      lulul.show()
//      xd.printSchema()
//      xd.show()
//  System.out.println("lul")
  //      gottem3.printSchema()
//      gottem3.show()
//      System.out.println("lul")
//    var lul = df.select("geometry.*","*")
//    lul.printSchema()
//    var xd = lul.withColumn("merged", struct(lul("type"), lul("coordinates")))
//    xd.createOrReplaceTempView("nice")
//    xd.show()
////    df.createOrReplaceTempView("inputtable")
////    df.show()
////    var gottem = df
//    var spatialDf = session.sql("""
//                                     | SELECT ST_GeomFromGeoJSON(merged) AS shape
//                                     | FROM nice
//                                   """.stripMargin)
//  spatialDf.show()
//  ST_GeomFromGeoJSON
//  spatialDf.printSchema()
//  System.out.println("fdffdfd")
////    val rawdf = session.read.format("json").option("header", "false").load(filepath)
////        rawdf.createOrReplaceTempView("suh")
////        rawdf.printSchema()
////        rawdf.show()
//
//    var df = session.read.format("json").option("header", "false").load(filepath).select("geometry")
//    var lul = df.select("geometry.*","*")
//        lul.createOrReplaceTempView("bitch")
//        lul.show()
////    var spatialdf = df.select()
//    ST_GeomFromWKT
////  var spatialDf = session.sql(
//      """
//        |SELECT ST_GeomFromWKT(geometry) AS countyshape
//        |FROM bitch
//      """.stripMargin)
//    spatialDf.createOrReplaceTempView("spatialdf")
//    spatialDf.show()

//  var luder = lul.withColumn("joined",concat(col("type"), lit(" "), col("coordinates")))
//  luder.createOrReplaceTempView("lol")
//  luder.show()
//    var xd = lul.select(concat(col("type"), col("coordinates")))
//    var xd = lul.withColumn("merged", struct(lul("type"), lul("coordinates")))
//        xd.createOrReplaceTempView("luererre")
//        xd.show()
//  lul.printSchema()
//        df.write.json("/f:/jåd")
//        df.createOrReplaceTempView("spatial")

////        df.show(5)
//    val Points = df.select("geometry").filter(col("geometry.type") === "Point").persist()
//    val Polygons = df.select("geometry").filter(col("geometry.type") === "Polygon" || col("geometry.type") === "MultiPolygon").persist()
////    print(Points.schema.size)
////    Points.printSchema()
////    Points.show(5)
//      df.foreach(x=>println(x))
//      Points.foreach(x=>println(x))
//  val allowTopologyInvalidGeometris = true // Optional
//  val skipSyntaxInvalidGeometries = false // Optional
//  val spatialRDD = GeoJsonReader.readToGeometryRDD(Points)//(session.sparkContext, Points, allowTopologyInvalidGeometris, skipSyntaxInvalidGeometries)//Is on  the same format as a WKT string: POLYGON[...], ... , POLYGON[...]
//  var pointRDD = Adapter.toSpatialRdd(Points, "geometry")
//    Points.buildIndex(IndexType.RTREE, false)
//  spatialRDD.buildIndex(IndexType.RTREE, false)
//    val geometryFactory = new GeometryFactory()
//    val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
//    val usingIndex = true
//    val result = KNNQuery.SpatialKnnQuery(spatialRDD, pointObject, 10, usingIndex)
//    polygonsRDD_ = Adapter.toSpatialRdd(Polygons, "geometry")

//    Polygons.printSchema()
//    Polygons.show(5)
//    val points = df.select("geometry").filter(entry => entry(1) == "Point")
//      val points = df.select("geometry","geometry.coordinates", "geometry.type").filter(x => x(1) == "Point")
//      val gottem = df.select("geometry").where("type = 'Point'")
//      val lul = df.select("geometry.type").filter(x => x(1) == "Point")
//      val dfstor = df.as("dfstor")
//      val filter = lul.as("dffilter")
//
//      val points = df.join(lul, col("") === col(), "inner")
//        points.printSchema()
//        points.show(5)
  //   points.printSchema()

//  lul.show(5)
//    println(points.take(5))
//    val points = session.sql("SELECT geometry FROM spatial WHERE geometry.type = Point")
//    val filesplitter = FileDataSplitter.GEOJSON
//    val points = new PointRDD(session.sparkContext, filepath, 0, filesplitter, true, StorageLevel.MEMORY_ONLY)
//    val poly = new PolygonRDD(session.sparkContext, filepath, 0, 0, filesplitter, true, StorageLevel.MEMORY_ONLY)
//    println(spatialRDD.countWithoutDuplicates())
//    println(points.countWithoutDuplicates())
//    println(poly.countWithoutDuplicates())
//    val points = spatialRDD.rawSpatialRDD.rdd.filter(x => x.getGeometryType == "Point")
//    val polygons = spatialRDD.rawSpatialRDD.rdd.filter(x => x.getGeometryType == "Point")
//    titty.foreach(println)
//    points.foreach()
    //    println(spatialRDD)


//    var spatialDf = Adapter.toDf(spatialRDD, session)
//    spatialDf.show(10)

//    val counddd = spatialDf.count()
//    print(counddd)
//    return spatialRDD

    //SpatialRDD[Geometry]
  }
//  def GetPoints(spatial: SpatialRDD[Geometry], session: SparkSession): SpatialRDD[Geometry] ={
//    val DF = Adapter.toDf(spatial, session)
////    print(DF.take(5))
////    DF.show(10)
//    val lul = DF.select("geometry.coordinates")
////    print(lul.take(5))
////    var gottem = new ShapefileRDD(session, "/f:/lululul.geojson")
//
////    var lul = new PolygonParser()
//
////    val getPointUDF = udf{ s: mutable.WrappedArray[String] => Point(Array(s(0).toDouble, s(1).toDouble)) }
//    val pointDF = DF.select("geometry.coordinates", "geometry.type").filter(entry => entry(1) == "Point").persist()
//    val result = Adapter.toSpatialRdd(pointDF, "coordinates")
//    return result
//
//
//  }
}
