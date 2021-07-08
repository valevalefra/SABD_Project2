import flink.query1.Query1Topology;
import flink.query2.Query2Topology;
import flink.query3.Query3Topology;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import utils.ComputeCell;
import utils.KafkaConfig;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class FlinkMain {

    private static final String CONSUMER_GROUP_ID = "single-flink-consumer";

    private final static String FORMAT_1 = "dd/M/yy HH:mm";
    private final static String FORMAT_2 = "dd-M-yy HH:mm";
    private final static String MATCH_1 = "([0]?[1-9]|[1|2][0-9]|[3][0|1])[/]([0]?[1-9]|[1][0-2])[/]([0-9]{4}|[0-9]{2})";
    private final static String MATCH_2 = "([0]?[1-9]|[1|2][0-9]|[3][0|1])[-]([0]?[1-9]|[1][0-2])[-]([0-9]{4}|[0-9]{2})";

    public static void main(String[] args) throws ParseException {



        // setup flink environment
        Configuration conf = new Configuration();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // add the source
        Properties props = KafkaConfig.getFlinkSourceProperties(CONSUMER_GROUP_ID);

        DataStream<Tuple2<Long, String>> stream = environment
                .addSource(new FlinkKafkaConsumer<>("flink-topic", new SimpleStringSchema(), props))
                // extract event timestamp and set it as key
                .flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<Long, String>> collector) {
                        Long eventTime = null;
                        String[] line = s.split(",");
                        DateFormat formatter1 = new SimpleDateFormat(FORMAT_1);
                        DateFormat formatter2 = new SimpleDateFormat(FORMAT_2);
                        String event = line[7];
                        try {
                            if (event.split(" ")[0].matches(MATCH_1)) eventTime = formatter1.parse(event).getTime();
                            else if (event.split(" ")[0].matches(MATCH_2)) eventTime = formatter2.parse(event).getTime();
                            collector.collect(new Tuple2<>(eventTime, s));
                        } catch (ParseException e) {
                        }
                    }
                })
                .filter(new FilterFunction<Tuple2<Long, String>>() {
                    @Override
                    public boolean filter(Tuple2<Long, String> tuple) throws Exception {

                        String[] line = tuple._2().split(",");
                        double longitude = Double.parseDouble(line[3]);
                        double latitude = Double.parseDouble(line[4]);
                        return (longitude<= ComputeCell.getMaxLongitude() && longitude>= ComputeCell.getMinLongitude())
                                && (latitude<= ComputeCell.getMaxLatitude() && latitude>= ComputeCell.getMinLatitude());
                    }
                })
                // assign timestamp to every tuple to enable watermarking system
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, String>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<Long, String> tuple) {

                        return tuple._1();
                    }
                })
                .name("stream-source");

        Query1Topology.buildTopology(stream);
        //Query2Topology.buildTopology(stream);
        //Query3Topology.buildTopology(stream);
        try {
            //execute the environment for DSP
            environment.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
