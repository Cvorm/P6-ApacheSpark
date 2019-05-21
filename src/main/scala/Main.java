import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Main {
    public static void main(String[] args) {
        //Logger.getLogger("org").setLevel(Level.OFF);
       // Logger.getLogger("akka").setLevel(Level.OFF);
        System.out.println("Program started");
        long startTime = System.nanoTime();
        Simba.main(new String[] {"resources/lululul.json","5"}); //args[0] new String[] {"resources/lululul.json","5"}
        long endTime = System.nanoTime();
        System.out.println("RUNTIME:" + ((endTime-startTime)/1000000) + " ms");
        System.out.println("Program ended");
    }
}
