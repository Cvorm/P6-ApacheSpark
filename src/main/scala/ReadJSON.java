/*
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Array;
import java.util.Map;

import com.google.gson.*;

public class ReadJSON {

   public void ParseBaby() {
      JsonParser parser = new JsonParser();
      InputStream inputSteam = getClass().getClassLoader().getResourceAsStream("sample.geojson");
      Reader reader = new InputStreamReader(inputSteam);
      /*parser.parse(reader);
      JsonElement rootElement = parser.parse(reader);
      JsonObject rootObject = rootElement.getAsJsonObject();
      JsonPrimitive timestamp = rootObject.getAsJsonPrimitive("timestamp");
      System.out.println(timestamp.toString());
      JsonArray features = rootObject.getAsJsonArray("features");
      JsonArray array = new JsonArray();
      for (JsonElement obj : features) {
          JsonObject fObj = obj.getAsJsonObject();
          JsonObject jsonObj = new JsonObject();
          JsonPrimitive type = fObj.getAsJsonPrimitive("type");
          JsonObject properties = fObj.getAsJsonObject("properties");
          JsonObject geometry = fObj.getAsJsonObject("geometry");
          JsonPrimitive id = fObj.getAsJsonPrimitive("id");

          System.out.println(fObj);
          array.add(fObj);
      }
      System.out.println(array);
   }
}
*/