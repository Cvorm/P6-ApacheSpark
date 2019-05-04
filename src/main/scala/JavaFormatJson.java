import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.io.IOException;
import java.util.Iterator;
import java.io.FileWriter;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import scala.collection.Iterator$;

public class JavaFormatJson {

    public void FormatGeoJson(String filepath){

        JSONParser parser = new JSONParser();

        try(Reader reader = new FileReader(filepath)){
            try (FileWriter file = new FileWriter("formattedGeo2.geojson")) {

                JSONObject jsonObj = (JSONObject) parser.parse(reader);
                JSONArray spatialData = (JSONArray) jsonObj.get("features");
//            Iterator<String> iterator = spatialData.iterator();
                JSONArray list = new JSONArray();
                for (int i = 0; i < spatialData.size(); i++) {
                    JSONObject val = (JSONObject) spatialData.get(i);
                    JSONObject lul = (JSONObject) val.get("geometry");
                    file.write(lul.toJSONString());
                    file.write(',');
                    file.write(System.lineSeparator());
                    list.add(lul);
                    list.add("\\n");
//                System.out.println("succ");
                }
            }
//            JSONObject result = new JSONObject();
//            try (FileWriter file = new FileWriter("formattedGeo.geojson")) {
//
//                file.write(list.toJSONString());
////                file.flush();
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            while(iterator.hasNext()){
//                String var = (iterator.next());
//                System.out.println(var);
//            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
