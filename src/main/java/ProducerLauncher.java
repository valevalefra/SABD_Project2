import kafka.SimpleKafkaProducer;
import utils.SpeedUpEventTime;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class ProducerLauncher {

    private final static String TOPIC = "t-singl-part";
    //private final static String TOPIC = "t-multi-part";
    private static final String CSV_PATH = "data/prj2_dataset.csv";

    private final static int NUM_MESSAGES = 1000;
    private final static int SLEEP = 1000;
    private final static String FORMAT_1 = "dd/M/yy HH:mm";
    private final static String FORMAT_2 = "dd-M-yy HH:mm";
    private final static String MATCH_1 = "([0]?[1-9]|[1|2][0-9]|[3][0|1])[/]([0]?[1-9]|[1][0-2])[/]([0-9]{4}|[0-9]{2})";
    private final static String MATCH_2 = "([0]?[1-9]|[1|2][0-9]|[3][0|1])[-]([0]?[1-9]|[1][0-2])[-]([0-9]{4}|[0-9]{2})";
    private static long min = Long.MAX_VALUE;
    private static long max = Long.MIN_VALUE;
    public static void main(String[] args) throws IOException, InterruptedException {

        SimpleKafkaProducer producer = new SimpleKafkaProducer(TOPIC);
        FileReader file = new FileReader(CSV_PATH);
        BufferedReader bufferedReader = new BufferedReader(file);
        String payload;
        Long eventTime = null;
        Long previous = null;
        int count = 0;
        long range = 1000L;
        ArrayList<SpeedUpEventTime> listEvent = new ArrayList<>();

        while ((payload = bufferedReader.readLine()) != null) {
            try {
                if (count==0){
                    count = 1;
                    continue;
                }
                if (count==1) {
                    String[] line = payload.split(",");
                    DateFormat formatter1 = new SimpleDateFormat(FORMAT_1);
                    DateFormat formatter2 = new SimpleDateFormat(FORMAT_2);
                    String event = line[7];
                    /**for (String matches:matchers){
                        if (event.split(" ")[0].matches(matches)) eventTime = formatter1.parse(event).getTime();
                        break;
                    }*/
                    if (event.split(" ")[0].matches(MATCH_1)) eventTime = formatter1.parse(event).getTime();
                    else if (event.split(" ")[0].matches(MATCH_2)) eventTime = formatter2.parse(event).getTime();
                    if (eventTime<min) min = eventTime;
                    else if (eventTime>max) max = eventTime;
                    //double proportion = range /(double)(max - min);
                    //System.out.println("proportion " + proportion);
                    SpeedUpEventTime newEvent = new SpeedUpEventTime(eventTime, payload);
                    listEvent.add(newEvent);
                       // System.out.println((long) ((eventTime -previous)* proportion));
                        //TimeUnit.MILLISECONDS.sleep((long) ((eventTime -previous)* proportion));
                       // System.out.println("divisione " + (long) ((eventTime - previous)*proportion));
                   // previous = eventTime;
                    //producer.produce(eventTime, payload);
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        double proportion = range /(double)(max - min);
        Collections.sort(listEvent);
        Long previousTime = null;
        for (SpeedUpEventTime speed: listEvent){
            Long event = speed.getEventTime();
            String line = speed.getPayload();
           // System.out.println("event time " + event + "previous time " + previousTime);
            if(previousTime != null){
            //System.out.println("division proportion " + (long) ((event -previousTime)* proportion));
            TimeUnit.MILLISECONDS.sleep((long) ((event -previousTime)* proportion));
            }
            previousTime = event;
            System.out.println("event " + event + " payload " + line);
            producer.produce(event, line);
        }
    }
}
