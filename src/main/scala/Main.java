import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import org.apache.spark.rdd.RDD;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator;
import org.datasyslab.geospark.spatialRDD.*;
import scala.Array;
import scala.Long;
import scala.collection.immutable.List;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.ListBuffer$;


public class Main {
    public static void main(String[] args) {                  //Use main args for passing data into the functions:
                                                              //1: Filepath to first input file(string)
                                                              //2: Filepath to second input file(string)
                                                              //3: Knn number of neighbors to find(Integer)
                                                              //4: x1 //5: y1 //6: x2 //7 y2

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        System.out.println("Program started");

//--------------------------------------------------ONLY RUN FOR FORMATTING DATA--------------------------------------------------
//        JavaFormatJson format = new JavaFormatJson();           //for making the fat data boi
//        format.FormatGeoJson("resources/sample.geojson");
//        format.FilterData("/f:/lululul.geojson");


//--------------------------------------------------CREATE SPARK AND RUN QUERIES--------------------------------------------------
        String inputFilepath = args[0];
        String inputFilepath2 = args[1];
        int k = Integer.parseInt(args[2]);
        double x1 = Double.parseDouble(args[3]);
        double y1 = Double.parseDouble(args[4]);
        double x2 = Double.parseDouble(args[5]);
        double y2 = Double.parseDouble(args[6]);

        long startTime = System.nanoTime();
        SparkSession spark = new GeoSpark().GeosparkSession();
        SpatialRDD points = new SpatialRDD();
        SpatialRDD polygons = new SpatialRDD();

        GeoSpark geo= new GeoSpark();

        points = geo.GEOJsonFromFileToRDD(inputFilepath, spark);
        Double countPoint = geo.GetElements();

        polygons = geo.GEOJsonFromFileToRDD(inputFilepath2, spark);
        Double countPolygon = geo.GetElements();

//        points = geo.GEOJsonFromFileToRDD("/f:/lululul.geojson", spark);
//        Double countPoint = geo.GetElements();
//
//        RunQueries gottem = new RunQueries();
//        gottem.KNNQueryWithRtree(points, spark, 10);

//        polygons = geo.GEOJsonFromFileToRDD(inputFilepath2, spark);
//        Double countPolygon = geo.GetElements();
//
        RunQueries firstRun = new RunQueries();
        RunQueries secondRun = new RunQueries();

        SpatialRDD pointsIndexed = firstRun.BuildRtreeIndex(points);
        firstRun.KNNQueryWithRtree(pointsIndexed, spark, k, x1, y1);
        firstRun.KNNQueryWITHOUTIndex(points, spark, k, x1, y1);
        firstRun.RangeQueryWithRtree(pointsIndexed, spark, x1, y1, x2, y2);
        firstRun.RangeQueryWITHOUTIndex(points, spark, x1, y1, x2, y2);

        SpatialRDD polygonsIndexed = secondRun.BuildRtreeIndex(polygons);
        secondRun.KNNQueryWithRtree(polygonsIndexed, spark, k, x1, y1);
        secondRun.KNNQueryWITHOUTIndex(polygons, spark, k, x1, y1);
        secondRun.RangeQueryWithRtree(polygonsIndexed, spark, x1, y1, x2, y2);
        secondRun.RangeQueryWITHOUTIndex(polygons, spark, x1, y1, x2, y2);
//        points = geo.GetPoints();
//        polygons = geo.GetPolygons();
//        RunQueries.KNNwithindex(points, spark, 10);
//        data = geo.GEOJsonFromFileToRDD("/f:/lululul.geojson", spark); ///f:/lululul.geojson
//        RunQueries.KNNwithindex(data, spark, 10);
//        points = geo.ToPoints(data);
//        polygons = geo.ToPolygons(data);
//        SpatialRDD pointsIndexed = RunQueries.BuildRtreeIndex(points);
//        SpatialRDD polygonsIndexed = RunQueries.BuildRtreeIndex(polygons);
//        RunQueries.KNNQueryWithRtree(pointsIndexed, spark, 10);
//        data = new GeoSpark().GEOJsonFromFileToRDD(inputFilepath, spark);
//        new Stark().LoadDataStark("resources/formattedGeo2.geojson", spark);
//        newdata = new GeoSpark().GEOJsonFromFileToRDD("/f:/lululul.geojson", spark);
//        SpatialRDD points = new SpatialRDD();
//        points = new GeoSpark().GetPoints(data,spark);
//        SpatialRDD pointsIndexed = RunQueries.BuildRtreeIndex(points);
//        RunQueries.KNNQueryWithRtree(pointsIndexed, spark, 10);
//--------------------------------------------------RESULTS--------------------------------------------------
//        SpatialRDD dataIndexed = RunQueries.BuildRtreeIndex(data);
////
//        RunQueries.KNNQueryWithRtree(dataIndexed,spark, 10);
//        RunQueries.RangeQueryWithRtree(dataIndexed, spark);
//

        long endTime = System.nanoTime();
        long[] first = firstRun.getRuntimes();
        long[] second = secondRun.getRuntimes();
 //
        System.out.println("##########################################################################");
        System.out.println("POINT-RTREE INDEX TIME:" + first[0]);
        System.out.println("POINT KNN QUERY INDEXED TIME:" + first[1]);
        System.out.println("POINT KNN QUERY WITHOUT INDEX  TIME:" + first[2]);
        System.out.println("POINT RANGE QUERY INDEXED  TIME:" + first[3]);
        System.out.println("POINT RANGE QUERY WITHOUT INDEX  TIME:" + first[4]);
        System.out.println("POINT COUNT:" + countPoint);
//        System.out.println("THROUGHPUT:" + (((endTime-startTime)/1000000)/count) + "elements");
        System.out.println("-------------------------------POLYGONS-------------------------------");

        System.out.println("POLYGON-RTREE INDEX TIME:" + second[0]);
        System.out.println("POLYGON KNN QUERY INDEXED TIME:" + second[1]);
        System.out.println("POLYGON KNN QUERY WITHOUT INDEX  TIME:" + second[2]);
        System.out.println("POLYGON RANGE QUERY INDEXED  TIME:" + second[3]);
        System.out.println("POLYGON RANGE QUERY WITHOUT INDEX  TIME:" + second[4]);
        System.out.println("POLYGON COUNT:" + countPolygon);
        System.out.println("##########################################################################");


        System.out.println("RUNTIME TOTAL:" + ((endTime-startTime)/1000000) + " ms");
        System.out.println("Program ended");

    }
}
