package flink.query3;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import utils.kafka_utils.FlinkStringToKafkaSerializer;
import config.Configuration;
import utils.beans.ShipData;
import utils.metrics.MetricsInvoker;

import java.text.SimpleDateFormat;

public class Query3Topology {

    /**
     * Function to build the topology and answer to third query.
     * @param source, DataStream containing tuples with as first value timestamp and
     *                record as the second one.
     */
    public static void buildTopology(DataStream<Tuple2<Long, String>> source) {

        /* Selecting required columns in order to obtain information
         * about latitude, longitude, timestamp and tripId.
         */
        DataStream<ShipData> stream = source
                .flatMap(new FlatMapFunction<Tuple2<Long, String>, ShipData>() {
                    @Override
                    public void flatMap(Tuple2<Long, String> tuple, Collector<ShipData> collector) {
                        ShipData data;
                        String[] info = tuple._2().split(",");

                        data = new ShipData(Double.parseDouble(info[3]), Double.parseDouble(info[4]), info[info.length - 1], tuple._1());
                        collector.collect(data);
                    }
                }).name("query3-selector");;

        // Assigning to the stream 1 hour windows
        stream.windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new DistanceAggregator(), new DistanceProcessWindow())
                .map(new Query3Topology.ResultMapper())
                .name("query3-1hour-distance")
                //.addSink(new MetricsInvoker())
                .addSink(new FlinkKafkaProducer<>(Configuration.FLINK_QUERY_3_1HOUR_TOPIC,
                        new FlinkStringToKafkaSerializer(Configuration.FLINK_QUERY_3_1HOUR_TOPIC),
                        Configuration.getFlinkSinkProperties("producer" +
                                Configuration.FLINK_QUERY_3_1HOUR_TOPIC),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query3-1hour-distance-sink");

        // Assigning to the stream 2 hours windows
        stream.windowAll(TumblingEventTimeWindows.of(Time.hours(2)))
                .aggregate(new DistanceAggregator(), new DistanceProcessWindow())
                .map(new Query3Topology.ResultMapper())
                .name("query3-2hour-distance")
                //.addSink(new MetricsInvoker())
                .addSink(new FlinkKafkaProducer<>(Configuration.FLINK_QUERY_3_2HOUR_TOPIC,
                        new FlinkStringToKafkaSerializer(Configuration.FLINK_QUERY_3_2HOUR_TOPIC),
                        Configuration.getFlinkSinkProperties("producer" +
                                Configuration.FLINK_QUERY_3_2HOUR_TOPIC),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query3-2hour-distance-sink");

    }

    /**
     * Inner class to extract records in such a way to be passed
     * to Kafka Consumer as required.
     */
    private static class ResultMapper implements MapFunction<DistanceOutcome, String> {
        @Override
        public String map(DistanceOutcome distanceOutcome) throws Exception {

            StringBuilder builder = new StringBuilder();
            SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy hh:mm");
            String date = sdf.format(distanceOutcome.getStartDate());
            builder.append(date);
            builder.append(";");
            for(int i=0; i<distanceOutcome.getRanking().size(); i++) {
                builder.append(distanceOutcome.getRanking().get(i)._1());
                builder.append(";");
                builder.append(distanceOutcome.getRanking().get(i)._2());
                if(i!=distanceOutcome.getRanking().size()-1) builder.append(";");
            }
            return builder.toString();
        }
    }
}
