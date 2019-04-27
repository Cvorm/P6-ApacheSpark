import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ReadJSON {
   public void ParseBaby() {
      JsonParser parser = new JsonParser();
      InputStream inputSteam = getClass().getClassLoader().getResourceAsStream("sample.geojson");
      Reader reader = new InputStreamReader(inputSteam);
      /*parser.parse(reader);*/
      JsonElement rootElement = parser.parse(reader);
      JsonObject rootObject = rootElement.getAsJsonObject();
      JsonObject type = rootObject.getAsJsonObject("type");
      System.out.println(rootObject.toString());

   }
}
