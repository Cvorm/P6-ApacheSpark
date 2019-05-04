//import org.apache.spark.{SparkContext, sql}
//import org.apache.spark.sql.simba.{Dataset, ShapeSerializer, SimbaSession}
//import org.apache.spark.sql.simba.spatial.{Point, Polygon, Shape}
//import org.apache.spark.sql.{SQLContext, simba}
//import org.apache.spark.sql.simba.index.RTreeType
//import java.util.ArrayList
//
//import org.apache.spark.sql.functions.explode
//import com.google.gson.Gson
//import com.sun.rowset.internal.Row
//import org.apache.spark.sql.simba.index.{RTreeType, TreapType}
//import org.apache.spark
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.simba.examples.SpatialClassInference
//import org.json4s.jackson.Json
//
//import scala.util.matching.Regex
//object Simba {
//
//  //case class PointData(x: Double, y:Double, z:Double, other:String)
//  case class PointData(p: Point, payload: Int)
//
//  //case class Polygon(points: Array[Point])
//
//  def Build(): Unit = {
//    val simbaSession = SimbaSession
//      .builder()
//      .master("local[4]")
//      .appName("SIMBA")
//      .config("simba.index.partitions", "64")
//      .getOrCreate()
//    /*
//    buildIndex(simbaSession)
//    useIndex1(simbaSession)
//    useIndex2(simbaSession)
//    runRangeQuery(simbaSession)
//    runKnnQuery(simbaSession)
//  */
//
//    //toSchemaRDD.write.json("types")
//    import simbaSession.implicits._
//    import simbaSession.simbaImplicits._
//
//   /* var result = input.flatMap(record =>
//      try {
//        people.add(mapper.readValue(line, Person.
//        class
//        ) ) lassOf[]
//      })*/
//    new Point(Array(1, 2, 1))
//      //simbaSession.stop()
//      //JsonReader
//  }
//}
//  object BasicSpatialOps {
//    import org.apache.spark.sql.simba.spatial.Polygon
//    case class PointData(x: Double, y: Double, z: Double, other: String)
//    case class Worm(coordinates: Array[Array[String]], `type`: String)
//    case class Polygon(`type`: String, coordinates: String)
//    case class Shape(coordinates: Array[Array[Point]], `type`: String)
//    case class Work(coordinates: Array[String])
//    def main(): Unit = {
//
//      val simbaSession = SimbaSession
//        .builder()
//        .master("local[4]")
//        .appName("SparkSessionForSimba")
//        .config("simba.join.partitions", "20")
//        .getOrCreate()
//
////      runRangeQuery(simbaSession)
//      runKnnQuery(simbaSession)
//      //runJoinQUery(simbaSession)
//      simbaSession.stop()
//    }
//    private def runKnnQuery(simba: SimbaSession): Unit = {
//      import scala.collection.mutable.WrappedArray
//      import simba.implicits._
//      import simba.simbaImplicits._
//      import java.util.regex.Pattern
//      import org.apache.spark.sql.functions.udf
//      import org.apache.spark.sql.functions.regexp_extract
//      import org.apache.spark.sql.functions.lit
//      def userpr = udf((s: String) => {
//        //new Point(s.split(","))
//      })
//
//
//      //simba.read.option("multiLine", true)
//      val df = simba.read.json("resources/output.geojson")
//      val test = df.select("features.geometry")
//
//      //val obj = Shape(test)
//      val ds: Dataset[Worm] = df.select("features.geometry.coordinates", "features.geometry.type").as[Worm]
//      val pattern: Regex = """(\\d*\\.\\d*),(\\d*\\.\\d*)*""".r
//      //val groups = df.select("features.geometry.coordinates").as[Point] //attern.findFirstMatchIn(ds.map(x => x.coordinates(0)).map(y => y(0)).toString())// (\d*\.\d......),(\d*\.\d......)
//      val waht = df.selectExpr("features.geometry.coordinates[1][0]")
////      waht.foreach(x=> println(x))
////      waht.show(10)
//      //val pattern(x, y) = waht.take(100)
////      println(waht)
////      val mapped = ds.map(x => x.coordinates(0)).map(y => y(0)).foreach(x => println(x.take(500)))//.map(x => x.trim.split(","))//.filter(x => x.toString.matches("[0-9]")) //.map(x => x.split(",")
//      //val mapped2 = mapped.foreach(x=> x.map(_.toDouble)) //println(pattern.findAllIn(x))) // "\\[(\\d*\\.\\d......),(\\d*\\.\\d......)", 1))
//      //mapped.flatMap(s => scala.util.Try(s.toDouble).toOption)
//      /*mapped match {
//        case pattern(x, y) => "$x, $y" case _ => "No match"
//      }*/
//
//      val dataframe = simba.read.json("resources/output.geojson")
//      val test2 = dataframe.select("features.geometry.coordinates", "features.geometry.type")
//      test2.show(10);
//
//      val dataframeCoor = dataframe.select(explode(dataframe("features.geometry.coordinates")))
//      val dataframeType = dataframe.select("features.geometry.type")
//
//      val countDf = dataframeType.count()
//      val dfPolygons = Nil
//      val arrayPoly[Worm] = Nil
//
//      for(count <- 0 to countDf){
//        arrayPoly += Worm(dataframeCoor.selectExpr("coordinates[count]"),dataframeType.selectExpr("type[count]"))
//      }
//
//      for(coor <- dataframeCoor("features.geometry.coordinates"))
//      dataframeCoor.show()
//
//      //mapped.printSchema()
//      //val mapped2 = mapped.filter(x=> x.matches("\\d*\\.\\d*")).collect().map(x => x.toDouble)
//      //mapped.take(200).foreach(x=> println(x(0), x(1), x(2)))
//      //mapped2.take(10).foreach(println)
//      //for((a,b)) <- ds.
//
//      //ds.limit(10).select("coordinates").take(10).foreach(println)
//      //pls.printSchema()
//     //pls.show(10)
//      //val ds2: Dataset[Shape] = df.select("features.geometry.coordinates", "features.geometry.type").as[Shape]
//      //val ds3 = ds.map(x => x.coordinates.toString.split(",")).map(_ to)
//      //ds.show(10)
//     // ds3.show(10)
//      //ds.printSchema()
//      //ds.foreach(x => Po)
//      //val cunt = df.select("features.geometry.coordinates").map(shape => Shape(Array(Array(shape.toString())),"fuck"))
//      //cunt.foreach(x=> println(x.c.toString))
//      //ds.knn(Array("coordinates[0]","coordinates[1]"),Array(1.0,1.0),4).show((4))
//      //cunt.printSchema()
//      //cunt.show(10)
//      //var pol = Polygon(df.select("features.geometry.type").toString(), df.select("features.geometry.coordinates").collect().map(row => row.toString()))
//      val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
//        PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS()
//      //val polygonClassDS = Seq(Polygon(Array(Point(13.6529596,51.1097375),Point(13.6530183,51.1098287),Point(13.6528717, 51.1098659),
//        //Point(13.652813,51.1097747), Point( 13.6529596,51.1097375))))
//      //val polygonClassDS = Polygon(Array(df.select("features.geometry.coordinates")))
//     //val test = df.flatMap(polygon => polygon.coordinates)
////      df.show(10)
//      //val result = RDD[Polygon] = df.filter()
//      //val what = df.flatMap(x => Polygon(x.toString()))
//      //val features = df.select("features")
//      //val f_flat = features.flatMap(x => print(x.toString()))
//      //val f_flat
//      //val features = df.select("features").withColumn("features.geometry", $"features.geometry.type")
//      //flattened.show(20)
//      //DF.map()
//      //df.map(record => record.Polygon(df.select($"features.geometry.type"), df.select($"features.geometry.coordinates")))
//      //val features = df.select($"features.geometry.type")
//      //val features2 = df.select($"features.geometry.coordinates")
//      //features.show(10)
////      df.show(10)
//
//
//      //val testvar = Polygon(features.select($"geometry.type").toString())
//      //val test = DF.rdd.map(_.getString(0))
//      //val newFeatures = features.as[Polygon]
//      //features.knn(Array("x", "y"),Array(1.0, 1.0),4).show(4)
//     // val pls = simba.read.json(test)
//      //pls.show(10)
//      //val test.toDS
//      //val newDF = features.map(x => Worm(x, x.toString))
//      //SUCC.foreach(item =>
//      /// println(item.toString())
//      //,Worm())
//      //DF.createOrReplaceTempView("a")
//      //SUCC.printSchema()
//      //DF.printSchema()
//      println("succ")
//      //DF.knn(Array("x","y"),Array(1.0,1.0),4).show((4))
//      //DF.knn
//
//
//
//
//
//      //caseClassDS.knn(Array("x", "y"),Array(1.0, 1.0),4).show(4)
//
//    }
//
//    private def runRangeQuery(simba: SimbaSession): Unit = {
//
//      import simba.implicits._
//      val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
//        PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS()
//
//      import simba.simbaImplicits._
//      caseClassDS.range(Array("x", "y"),Array(1.0, 1.0),Array(3.0, 3.0)).show(10)
//
//    }
//
//    private def runJoinQUery(simba: SimbaSession): Unit = {
//
//      import simba.implicits._
//
//      val DS1 = (0 until 10000).map(x => PointData(x, x + 1, x + 2, x.toString)).toDS
//      val DS2 = (0 until 10000).map(x => PointData(x, x, x + 1, x.toString)).toDS
//
//      import simba.simbaImplicits._
//
//      DS1.knnJoin(DS2, Array("x", "y"),Array("x", "y"), 3).show()
//
//      DS1.distanceJoin(DS2, Array("x", "y"),Array("x", "y"), 3).show()
//
//    }
//  }
//object IndexExample {
//  case class PointData(x: Double, y: Double, z: Double, other: String)
//
//  def main(): Unit = {
//    val simbaSession = SimbaSession
//      .builder()
//      .master("local[4]")
//      .appName("IndexExample")
//      .config("simba.index.partitions", "64")
//      .getOrCreate()
//
//    buildIndex(simbaSession)
//    //useIndex1(simbaSession)
//   // useIndex2(simbaSession)
//    //simbaSession.stop()
//  }
//
//  private def buildIndex(simba: SimbaSession): Unit = {
//    import simba.implicits._
//    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
//      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS
//    val DF = simba.read.json( "resources/output.geojson")
//    DF.createOrReplaceTempView("a")
//
//    simba.indexTable("a", RTreeType, "testqtree",  Array("x", "y") )
//
//    simba.showIndex("a")
//  }
//
//  private def useIndex1(simba: SimbaSession): Unit = {
//    import simba.implicits._
//    import simba.simbaImplicits._
//    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
//      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDF()
//
//    datapoints.createOrReplaceTempView("b")
//
//    simba.indexTable("b", RTreeType, "RtreeForData",  Array("x", "y") )
//
//    simba.showIndex("b")
//
//    val res = simba.sql("SELECT * FROM b")
//    res.knn(Array("x", "y"),Array(1.0, 1.0),4).show(4)
//
//  }
//
//  private def useIndex2(simba: SimbaSession): Unit = {
//    import simba.implicits._
//    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
//      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDF()
//
//    datapoints.createOrReplaceTempView("b")
//
//    simba.indexTable("b", RTreeType, "RtreeForData",  Array("x", "y") )
//
//    simba.showIndex("b")
//
//    simba.sql("SELECT * FROM b where b.x >1 and b.y<=2").show(5)
//
//  }
//
//  private def useIndex3(simba: SimbaSession): Unit = {
//    import simba.implicits._
//    val datapoints = Seq(PointData(0.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
//      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS()
//
//    import simba.simbaImplicits._
//
//    datapoints.index(TreapType, "indexForOneTable",  Array("x"))
//
//    datapoints.range(Array("x"),Array(1.0),Array(2.0)).show(4)
//  }
//}