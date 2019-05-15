import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.index.RTreeType

object Simba {
  import org.apache.spark.sql.simba.spatial.{Point, Polygon}
  import org.apache.spark.sql.simba.{SimbaSession}
  import scala.util.matching.Regex
    case class Pointy(x: Double, y: Double)
    //case class PointData(x: Double, y: Double, z:Double, other: String)
    case class PointData(x: Double, y: Double)
    case class PointData2(value: Seq[PointData])
    case class Polygony(value: Array[Pointy])

    def main(): Unit = {
      val simbaSession = SimbaSession
        .builder()
        .master("local[4]")
        .appName("SparkSessionForSimba")
        .config("simba.join.partitions", "20")
        //.config("spark.serializer", classOf[KryoSerializer].getName)
        //.config()
        //.config("spark.kryo.registrator", classOf[TestRegistrator].getName)
        .getOrCreate()
      /* Load data for Polygon test */
      val data = GetPolygons(simbaSession)
      RunKNNPolygon(simbaSession, data)
      RunRangePolygon(simbaSession, data)

      /*Load data for Point test */
      val data2 = GetPoints(simbaSession)
      RunKNNPoint(simbaSession, data2)
      RunRangePoint(simbaSession, data2)
      RunIndexPoint2(simbaSession, data2)
      simbaSession.stop()
    }
  def RunKNNPolygon(simba: SimbaSession, df: Dataset[Polygon]): Unit = {
    val t1 = System.currentTimeMillis()
    df.knn("value", Array(15.0, 14.1), 3).show(3,false)
    val t2 = System.currentTimeMillis()
    println("RUNTIME POLYGON KNN: " + (t2 - t1))
  }
  def RunRangePolygon(simba: SimbaSession, df: Dataset[Polygon]): Unit = {
    val t1 = System.currentTimeMillis()
    df.range("value",Array(13.7, 50.0),Array(16.0, 52.0)).show(10,false)
    val t2 = System.currentTimeMillis()
    println("RUNTIME POLYGON KNN: " + (t2 - t1))
  }
  def RunKNNPoint(simba: SimbaSession, df: Dataset[Point]): Unit = {
    val t1 = System.currentTimeMillis()
    df.knn("value", Array(15.0, 14.1), 3).show(3,false)
    val t2 = System.currentTimeMillis()
    println("RUNTIME POINT KNN: " + (t2 - t1))
  }
  def RunRangePoint(simba: SimbaSession, df: Dataset[Point]): Unit = {
    val t1 = System.currentTimeMillis()
    df.range("value",Array(13.7, 50.0),Array(16.0, 52.0)).show(10,false)
    val t2 = System.currentTimeMillis()
    println("RUNTIME POINT KNN: " + (t2 - t1))
  }
  def RunIndexPoint(simba: SimbaSession, df: Dataset[Point]): Unit = {
    val t1 = System.currentTimeMillis()
    df.index(RTreeType, "indexname", Array("value")).show(10,false)
    val t2 = System.currentTimeMillis()
    println("RUNTIME POINT INDEX (T-TREE): " + (t2 - t1))
  }
  def RunIndexPoint2(simba: SimbaSession, df: Dataset[Point]): Unit = {
    val t1 = System.currentTimeMillis()
    df.index(RTreeType, "indexname", Array("value")).show(10,false)
    val t2 = System.currentTimeMillis()
    println("RUNTIME POINT INDEX (T-TREE): " + (t2 - t1))
    val t3 = System.currentTimeMillis()
    df.knn("value", Array(15.0, 14.1), 3).show(3,false)
    val t4 = System.currentTimeMillis()
    println("RUNTIME POINT INDEX (T-TREE) - knn query: " + (t4 - t3))
    val t5 = System.currentTimeMillis()
    df.range("value",Array(13.7, 50.0),Array(16.0, 52.0)).show(10,false)
    val t6 = System.currentTimeMillis()
    println("RUNTIME POINT INDEX (T-TREE) - range query: " + (t6 - t5))

  }

  def GetPolygons(simba: SimbaSession): Dataset[Polygon] = {
    import simba.implicits._
    import simba.simbaImplicits._
    val r: Regex = raw"""(\d*\.\d*),(\d*\.\d*)""".r
    val test = simba.read.json("resources/output.geojson").printSchema()
    val df = simba.read.json("resources/formattedGeo2.geojson").filter(x=>x(1) == "Polygon").map({
      x=>
        val g = r.findAllIn(x.toString()).matchData map {m => Pointy(m.group(1).toDouble,m.group(2).toDouble)}
        //val pointa = g.toArray[PointData].map(x=> Point(Array(x.x, x.y)))
        val pointArray = g.toArray[Pointy]
        pointArray :+ pointArray(0)
    }).as[Array[Pointy]]
    val pay = df.as[Polygony].collect()
    val dsPolygon2 = (0 until pay.length).map({ x =>
      Polygon(pay(x).value.map(i=> Point(Array(i.x, i.y))))
    }).toDS()
    return dsPolygon2
  }
  /*def GetPolygons(simba: SimbaSession) : Unit = {
    import simba.implicits._
      import simba.simbaImplicits._
    val r: Regex = raw"""(\d*\.\d*),(\d*\.\d*)""".r
    val df2 = simba.read.json("resources/test.json").as[Polygony]//.collect()(0).getInt(0)
    val df = simba.read.json("resources/formattedGeo2.geojson").filter(x=>x(1) == "Polygon").map({
        x=>
        val g = r.findAllIn(x.toString()).matchData map {m => Pointy(m.group(1).toDouble,m.group(2).toDouble)}
        //val pointa = g.toArray[PointData].map(x=> Point(Array(x.x, x.y)))
          val pointArray = g.toArray[Pointy]
          pointArray :+ pointArray(0)
      }).as[Array[Pointy]] //.write.json("resources/succ.geojson") //.show(10, false)
       /*val p = df2.collect()
       val dsPolygon = (0 until p.length).map({ x =>
         Polygon(p(x).value.map(i=> Point(Array(i.x, i.y))))
       }).toDS()*/
       val pay = df.as[Polygony].collect()
       val dsPolygon2 = (0 until pay.length).map({ x =>
         Polygon(pay(x).value.map(i=> Point(Array(i.x, i.y))))
       }).toDS()
       dsPolygon2.knn("value", Array(15.0, 14.1), 3).show(3,false)
       dsPolygon2.range("value",Array(13.7, 50.0),Array(16.0, 52.0)).show(10,false)
       //dsPolygon.knn("value", Array(15.0, 14.1), 3).show(3,false)
       //dsPolygon.range("value",Array(13.7, 50.0),Array(16.0, 52.0)).show(10,false)
    }*/
  def GetPoints(simba: SimbaSession) : Dataset[Point] = {
    import simba.implicits._
    import simba.simbaImplicits._
    val r: Regex = raw"""(\d*\.\d*),(\d*\.\d*)""".r
    val df = simba.read.json("resources/formattedGeo2.geojson").filter(x=>x(1) == "Polygon").map({
      x=>
        val g = r.findAllIn(x.toString()).matchData map {m => Pointy(m.group(1).toDouble,m.group(2).toDouble)}
        //val pointa = g.toArray[PointData].map(x=> Point(Array(x.x, x.y)))
        val pointArray = g.toArray[Pointy]
        pointArray :+ pointArray(0)
    }).as[Array[Pointy]] //.write.json("resources/succ.geojson") //.show(10, false)
    val p = df.as[Polygony].collect()
    val dsPoint = (0 until p.length).map({ x=>
      val tmp = (0 until p(x).value.length).map(y => Point(Array(p(x).value(y).x, p(x).value(y).y)))
      //tmp.toDS().index(RTreeType, "p", Array("value"))
      tmp.toArray
    }).flatMap(d=>d).toDS()
    dsPoint
    //dsPoint.index(RTreeType, "indexname", Array("value")).show(10,false)
  }

    /*def FormatData(simba: SimbaSession):Unit = {
      import simba.implicits._
      import simba.simbaImplicits._
      import org.apache.spark.sql.functions._
      val r: Regex = raw"""(\d*\.\d*),(\d*\.\d*)""".r //Reg.exp used for getting string X and Y values
      val df = simba.read.json("resources/output.geojson") //Load data
      val len = df.select(explode($"features")).count().toInt -1 //Lenght of DataFrame, used in loop
      val dfPoly = (1 until len).map({ //Maps each feature to a polygon
        x =>
          val ds = df.selectExpr(s"features[$x].geometry.coordinates").map({ //Selects x features coordinates
            y =>
            val g = r.findAllIn(y.toString()) //Finds all Points in row
            g.hasNext
            val points = for (j <- Array.range(0,  r.findAllIn(y.toString()).length , 1)) yield {
              var point = Pointy(g.group(1).toDouble,g.group(2).toDouble)
              g.next()
              point
            }
            points :+ points(0) //Adds first point to end, such that it satisfies polygon criteria
          })
         // val i = ds.index(RTreeType,"test", Array("value"))
          val k = ds.collect.flatMap(x=> x.map(y => Point(Array(y.x, y.y)))) //Collects and flattens dataframe created
          Polygon(k)
      }).toDS()
      dfPoly.printSchema()
      dfPoly.show(10,false)
      //dfPoly.knn("value", Array(15.0, 14.0), 3).show(10,false)
      //return dfPoly
    }*/
  }


