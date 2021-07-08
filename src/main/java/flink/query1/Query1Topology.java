package flink.query1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import utils.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Query1Topology {

    public static double canaleDiSiciliaLon = 11.797696;


    /**
     * Function to build the topology and answer to first query.
     * @param source, DataStream containing tuples with as first value timestamp and
     *                record as the second one.
     */
    public static void buildTopology(DataStream<Tuple2<Long, String>> source)  {

        /* Selecting required columns and filtering by longitude in order to
         * consider the Occidental Mediterranean Sea.
         */
        DataStream<ShipData> stream = source
                .flatMap(new FlatMapFunction<Tuple2<Long, String>, ShipData>() {
                    @Override
                    public void flatMap(Tuple2<Long, String> tuple, Collector<ShipData> collector) {
                        ShipData data;
                        String[] info = tuple._2().split(",");
                        data = new ShipData(Double.parseDouble(info[3]), Double.parseDouble(info[4]), Integer.parseInt(info[1]), tuple._1());
                        collector.collect(data);
                    }
                })
                .name("query1-selector")
                .filter(new FilterFunction<ShipData>() {
                    @Override
                    public boolean filter(ShipData shipData) throws Exception {
                        return (shipData.getLongitude()<=canaleDiSiciliaLon);
                    }
                }).name("query1-filter");

        // Stream partitioned by idCell
        KeyedStream<ShipData, String> keyedStream = stream.keyBy(ShipData::getIdCell);

        // Assigning to the stream seven days windows
        keyedStream.window(TumblingEventTimeWindows.of(Time.days(7)))
                   .aggregate(new AverageShipsAggregator(), new AverageProcessWindow())
                   .name("query1-weekly-mean")
                   .map(new ResultMapper())
                   .addSink(new FlinkKafkaProducer<>(KafkaConfig.FLINK_QUERY_1_WEEKLY_TOPIC,
                     new FlinkStringToKafkaSerializer(KafkaConfig.FLINK_QUERY_1_WEEKLY_TOPIC),
                     KafkaConfig.getFlinkSinkProperties("producer" +
                     KafkaConfig.FLINK_QUERY_1_WEEKLY_TOPIC),
                     FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                    .name("query1-weekly-mean-sink");


        // Assigning to the stream month windows
        keyedStream.window(TumblingEventTimeWindows.of(Time.days(28), Time.days(OutputUtils.OFFSET_MONTHLY+4)))
                   .aggregate(new AverageShipsAggregator(), new AverageProcessWindow())
                   .name("query1-monthly-mean")
                   .map(new ResultMapper())
                   .addSink(new FlinkKafkaProducer<>(KafkaConfig.FLINK_QUERY_1_MONTHLY_TOPIC,
                    new FlinkStringToKafkaSerializer(KafkaConfig.FLINK_QUERY_1_MONTHLY_TOPIC),
                    KafkaConfig.getFlinkSinkProperties("producer" +
                    KafkaConfig.FLINK_QUERY_1_MONTHLY_TOPIC),
                    FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                    .name("query1-monthly-mean-sink");
    }

    /**
     * Inner class to extract records in such a way to be passed
     * to Kafka Consumer as required.
     */
    private static class ResultMapper implements MapFunction<AverageOutcome, String> {

        @Override
        public String map(AverageOutcome averageOutcome) throws Exception {

            Double avg35 = 0.0;
            Double avg6069 = 0.0;
            Double avg7079 = 0.0;
            Double avgO = 0.0;
            StringBuilder builder = new StringBuilder();

            SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
            String date = sdf.format(averageOutcome.getStartDate());

            builder.append(date);
            builder.append(";");
            builder.append(averageOutcome.getIdCell());
            builder.append(";");

            for(Query1Result result : averageOutcome.getList()) {

                if (result.getShip35() != 0L && result.getAvg35() != 0.0) {
                    avg35 = result.getAvg35();
                }
                if (result.getShip6069() != 0L && result.getAvg6069() != 0.0) {
                    avg6069 = result.getAvg6069();
                }
                if (result.getShip7079() != 0L && result.getAvg7079() != 0.0) {
                    avg7079 = result.getAvg7079();
                }
                if (result.getShipO() != 0L && result.getAvgO() != 0.0) {
                    avgO = result.getAvgO();
                }
            }
                builder.append("Military");
                builder.append(";");
                builder.append(avg35);
                builder.append(";");
                builder.append("Passenger");
                builder.append(";");
                builder.append(avg6069);
                builder.append(";");
                builder.append("Cargo");
                builder.append(";");
                builder.append(avg7079);
                builder.append(";");
                builder.append("Other");
                builder.append(";");
                builder.append(avgO);

            return builder.toString();
        }
    }
}
