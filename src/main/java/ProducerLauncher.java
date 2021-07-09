
import flink.query1.Query1Topology;
import kafka.SimpleKafkaProducer;
import utils.beans.SpeedUpEventTime;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Class used to start a producer that reads from file and sends tuples to Kafka topics
 */

public class ProducerLauncher {

    private final static String TOPIC = "t-singl-part";
    private static final String CSV_PATH = "data/prj2_dataset.csv";


    private final static String FORMAT_1 = "dd/M/yy HH:mm";
    private final static String FORMAT_2 = "dd-M-yy HH:mm";
    private final static String MATCH_1 = "([0]?[1-9]|[1|2][0-9]|[3][0|1])[/]([0]?[1-9]|[1][0-2])[/]([0-9]{4}|[0-9]{2})";
    private final static String MATCH_2 = "([0]?[1-9]|[1|2][0-9]|[3][0|1])[-]([0]?[1-9]|[1][0-2])[-]([0-9]{4}|[0-9]{2})";
    private static long min = Long.MAX_VALUE;
    private static long max = Long.MIN_VALUE;


    public static void main(String[] args) throws IOException, InterruptedException {

        //Create producer
        SimpleKafkaProducer producer = new SimpleKafkaProducer(TOPIC);

        FileReader file = new FileReader(CSV_PATH);
        BufferedReader bufferedReader = new BufferedReader(file);
        String payload;
        Long eventTime = null;
        int count = 0;
        long range = 1000L;
        ArrayList<SpeedUpEventTime> listEvent = new ArrayList<>();

        while ((payload = bufferedReader.readLine()) != null) {
            try {
                //Ignore header
                if (count==0){
                    count = 1;
                    continue;
                }
                if (count==1) {
                    String[] line = payload.split(",");
                    DateFormat formatter1 = new SimpleDateFormat(FORMAT_1);
                    DateFormat formatter2 = new SimpleDateFormat(FORMAT_2);
                    String event = line[7];
                    if (event.split(" ")[0].matches(MATCH_1)) eventTime = formatter1.parse(event).getTime();
                    else if (event.split(" ")[0].matches(MATCH_2)) eventTime = formatter2.parse(event).getTime();
                    if (eventTime<min) min = eventTime;
                    else if (eventTime>max) max = eventTime;
                    SpeedUpEventTime newEvent = new SpeedUpEventTime(eventTime, payload);
                    listEvent.add(newEvent);
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        double proportion = range /(double)(max - min);
        //Order events by timestamp
        Collections.sort(listEvent);

        Long previousTime = null;
        int countOccidental = 0;
        int countOriental = 0;
        String occidentalFirstDate = null;
        String orientalFirstDate = null;
        for (SpeedUpEventTime speed: listEvent){
            Long event = speed.getEventTime();
            String line = speed.getPayload();
            if(previousTime != null){
            //Simulation of real data stream processing
            TimeUnit.MILLISECONDS.sleep((long) ((event -previousTime)* proportion));
            }
            previousTime = event;
            DateFormat formatterEvent = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            String date = formatterEvent.format(new Date(event));
            double longitude = Double.parseDouble(line.split(",")[3]);
            // Memorizing first date tuple for the Occidental Mediterranean Sea
            if(longitude <= Query1Topology.canaleDiSiciliaLon && countOccidental==0){
                occidentalFirstDate = date;
                countOccidental++;
            }
            // Memorizing first date tuple for the Oriental Mediterranean Sea
            else if (longitude > Query1Topology.canaleDiSiciliaLon && countOriental==0) {
                orientalFirstDate = date;
                countOriental++;
            }
            producer.produce(line);
        }

        // Creating a JSONObject containing first dates for the two Mediterranean seas
        JSONObject dates = new JSONObject();
        dates.put("dateOccidental", occidentalFirstDate);
        dates.put("dateOriental", orientalFirstDate);
        try (FileWriter fileJson = new FileWriter("src/main/java/utils/queries_utils/dates.json")) {
            fileJson.write(dates.toString());
            fileJson.flush();
        }catch (IOException e) {
            System.err.println("Error in writing the Json file");
        }
        producer.close();}
}
