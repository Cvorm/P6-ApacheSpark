import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.ArrayList;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator;
import org.datasyslab.geospark.spatialRDD.*;


public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        System.out.println("Program started");

//        JavaFormatJson format = new JavaFormatJson();           /for making the fat data boi
//        format.FormatGeoJson("resources/sample.geojson");

        SparkSession spark = new GeoSpark().GeosparkSession();
        SpatialRDD data = new SpatialRDD();
//        SpatialRDD starkData = new SpatialRDD();
        data = new GeoSpark().GEOJsonFromFileToRDD("resources/formattedGeo2.geojson", spark);
//        new Stark().LoadDataStark("resources/formattedGeo2.geojson", spark);

        SpatialRDD dataIndexed = new RunQueries().BuildRtreeIndex(data);

        new RunQueries().KNNQueryWithRtree(dataIndexed);
        new RunQueries().RangeQueryWithRtree(dataIndexed, spark);

//
//
//        GeoSpark lul = new GeoSpark();



//        /*Load input data into JsonArray*/
//        ReadJSON p = new ReadJSON();
////        ArrayList data = p.LoadData();
//        //System.out.println(data);
//        //p.LoadDataSimba();
//        /*Load data into RDD*/
//        BasicSpatialOps.main();
//        //IndexExample.main();
        System.out.println("Program ended");

    }
//    public JavaSparkContext StartSpark(){
//        /*Create a Java Spark Context */
//        SparkConf conf = new SparkConf().setAppName("SPARK").setMaster("local[*]");
//         return new JavaSparkContext(conf);
//    }
//    public JavaRDD LoadRDD(ArrayList data, JavaSparkContext sc) {
//        JavaRDD<ArrayList> rdd = sc.parallelize(data);
//        return rdd;
//    }
}
