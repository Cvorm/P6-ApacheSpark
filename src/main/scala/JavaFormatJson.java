import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.io.IOException;
import java.util.Iterator;
import java.io.FileWriter;


import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import scala.collection.Iterator$;

public class JavaFormatJson {

    private SpatialRDD points_;
    private SpatialRDD polygons_;

    public void FormatGeoJson(String filepath){

        JSONParser parser = new JSONParser();

        try(Reader reader = new FileReader(filepath)){
            try (FileWriter file = new FileWriter("formattedGeo5.geojson")) {

                JSONObject jsonObj = (JSONObject) parser.parse(reader);
                JSONArray spatialData = (JSONArray) jsonObj.get("features");

                JSONArray list = new JSONArray();
                for (int i = 0; i < spatialData.size(); i++) {
                    JSONObject val = (JSONObject) spatialData.get(i);
                    JSONObject lul = (JSONObject) val.get("geometry");
                    file.write(lul.toJSONString());
                    file.write(',');
                    file.write(System.lineSeparator());
                    list.add(lul);
                    list.add("\\n");

                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
    public void FilterData(String filepath){
        JSONParser parser = new JSONParser();

        JSONArray points = new JSONArray();
        JSONArray polygons = new JSONArray();

        try(Reader reader = new FileReader(filepath)){

            Object jsonObj =  parser.parse(reader);
            JSONArray items = (JSONArray) jsonObj;
            for(int i = 0; i < items.size(); i++){
                JSONObject val = (JSONObject) items.get(i);
                if(val.get("geometry.type") == "Point"){
                    points.add(val);
                }
                else if(val.get("geometry.type") == "Polygon") {
                    polygons.add(val);
                }
//                JSONObject lul = (JSONObject) val.get("ge");
            }
            System.out.println("lul");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
