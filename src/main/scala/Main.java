import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Main {
    public static void main(String[] args) {
        //Logger.getLogger("org").setLevel(Level.OFF);
       // Logger.getLogger("akka").setLevel(Level.OFF);
        System.out.println("Program started");
        long startTime = System.nanoTime();
        Simba.main(args); //new String[] {"resources/lululul.json","5","20"} new String[] {"resources/xab","5","20"} //new String[]{"resources/xab","resources/xaa","5", "20"}
        long endTime = System.nanoTime();
        System.out.println("RUNTIME:" + ((endTime-startTime)/1000000) + " ms");
        System.out.println("Program ended");
    }
}
