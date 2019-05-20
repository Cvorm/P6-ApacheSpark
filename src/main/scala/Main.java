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
    public static void main(String[] args) {                  //Use main args for passing data into the functions:
                                                              //1: Filepath to input file(string)
                                                              //2: Knn number of neighbors to find(Integer)
                                                              //3:
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        System.out.println("Program started");

//--------------------------------------------------ONLY RUN FOR FORMATTING DATA--------------------------------------------------
//        JavaFormatJson format = new JavaFormatJson();           //for making the fat data boi
//        format.FormatGeoJson("resources/sample.geojson");


//--------------------------------------------------CREATE SPARK AND RUN QUERIES--------------------------------------------------
        String inputFilepath = args[0];
        int k = Integer.parseInt(args[1]);

        long startTime = System.nanoTime();
        SparkSession spark = new GeoSpark().GeosparkSession();
        SpatialRDD data = new SpatialRDD();

//        data = new GeoSpark().GEOJsonFromFileToRDD("/f:/lululul.geojson", spark); ///f:/lululul.geojson
        data = new GeoSpark().GEOJsonFromFileToRDD(inputFilepath, spark);
//        new Stark().LoadDataStark("resources/formattedGeo2.geojson", spark);

//--------------------------------------------------INDEX INTO R-TREE USING GEOSPARK AND RUN KNN AND RANGE QUERY--------------------------------------------------
        SpatialRDD dataIndexed = new RunQueries().BuildRtreeIndex(data);

        new RunQueries().KNNQueryWithRtree(dataIndexed,spark, k);
        new RunQueries().RangeQueryWithRtree(dataIndexed, spark);

        long endTime = System.nanoTime();
        System.out.println("RUNTIME TOTAL:" + ((endTime-startTime)/1000000) + " ms");
        System.out.println("Program ended");
    }
}
