
import flink.query1.Query1Topology;
import kafka.SimpleKafkaProducer;
import utils.SpeedUpEventTime;
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
 * Class used to start a producer that reads from file and send tuples to Kafka topics
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


    public static void main(String[] args) throws IOException, InterruptedException, ParseException {

        //Create producer
        SimpleKafkaProducer producer = new SimpleKafkaProducer(TOPIC);

        FileReader file = new FileReader(CSV_PATH);
        BufferedReader bufferedReader = new BufferedReader(file);
        String payload;
        Long eventTime = null;
        int count = 0;
        long range = 1000L;
        ArrayList<SpeedUpEventTime> listEvent = new ArrayList<>();
        //List<Date> set = new ArrayList<>();
        TreeMap<String, Double> occidentalMap = new TreeMap<>();
        TreeMap<String, Double> orientalMap = new TreeMap<>();

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
            Double longitude = Double.parseDouble(line.split(",")[3]);
            if(longitude <= Query1Topology.canaleDiSiciliaLon){
                 occidentalMap.put(date, longitude);
            }
            else{
                orientalMap.put(date, longitude);
            }
            //set.add((new Date(event)));
            producer.produce(line);
        }
      /*  //Sorting
        Collections.sort(set, new Comparator<Date>() {
            @Override
            public int compare(Date lhs, Date rhs) {
                if (lhs.getTime() < rhs.getTime())
                    return -1;
                else if (lhs.getTime() == rhs.getTime())
                    return 0;
                else
                    return 1;
            }
        });*/
        System.out.println(occidentalMap.firstEntry());
        System.out.println(orientalMap.firstEntry());
        JSONObject dates = new JSONObject();
        dates.put("dateOccidental", occidentalMap.firstEntry().getKey());
        dates.put("dateOriental", orientalMap.firstEntry().getKey());
        FileWriter fileJson = new FileWriter("Results/dates.json");
        fileJson.write(dates.toString());
        fileJson.flush();


        //OutputUtils.takeOffset(set.stream());
        producer.close();}
}
