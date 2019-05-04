//
//import org.apache.spark.sql.SparkSession
//
//import scala.util.matching.Regex
//
//class FormatJson {
//  def FormatDataSama(session: SparkSession, filepath: String): Unit ={
////    val r: Regex = raw"""(?<=geometry":)(.*)(?="i)""".r
////    val reg = raw"""(?<=geometry":)(.*)(?="i)""".r
//    val dataframe = session.read.json(filepath)
//    val dataSize = dataframe.select("features").count().toInt
//    val strArray = new Array[String](dataSize)
//    for(i <- 0 to dataSize){
//      val ds = dataframe.selectExpr(s"features[$i]").map({
//        str =>
//          val reg = raw"""(?<=geometry":)(.*)(?="i)""".r
//          val group = reg.findAllIn(str.toString())
//
//      })
//
//    }
//  }
//}
