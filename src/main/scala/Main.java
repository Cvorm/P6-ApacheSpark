import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) {
        System.out.println("Program started");
        /*Load input data into JsonArray*/
        ReadJSON p = new ReadJSON();
        ArrayList data = p.LoadData();
        //System.out.println(data);
        //p.LoadDataSimba();
        /*Load data into RDD*/
        BasicSpatialOps.main();
        //IndexExample.main();
        System.out.println("Program ended");

    }
    public JavaSparkContext StartSpark(){
        /*Create a Java Spark Context */
        SparkConf conf = new SparkConf().setAppName("SPARK").setMaster("local[*]");
         return new JavaSparkContext(conf);
    }
    public JavaRDD LoadRDD(ArrayList data, JavaSparkContext sc) {
        JavaRDD<ArrayList> rdd = sc.parallelize(data);
        return rdd;
    }
}
