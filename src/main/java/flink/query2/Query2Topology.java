package flink.query2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import utils.beans.ShipData;
import utils.kafka_utils.FlinkStringToKafkaSerializer;
import config.Configuration;
import utils.metrics.MetricsInvoker;
import utils.queries_utils.ResultsUtils;

import java.text.SimpleDateFormat;


public class Query2Topology {

    /**
     * Function to build the topology and answer to second query.
     * @param source, DataStream containing tuples with as first value timestamp and
     *                record as the second one.
     */
    public static void buildTopology(DataStream<Tuple2<Long, String>> source) {

        /* Selecting required columns in order to obtain information
         * about latitude, longitude, timestamp and shipId.
         */
        DataStream<ShipData> stream = source
                .flatMap(new FlatMapFunction<Tuple2<Long, String>, ShipData>() {
                    @Override
                    public void flatMap(Tuple2<Long, String> tuple, Collector<ShipData> collector) {
                        ShipData data;
                        String[] info = tuple._2().split(",");

                        data = new ShipData(Double.parseDouble(info[3]), Double.parseDouble(info[4]), tuple._1(), info[0]);
                        collector.collect(data);
                    }
                }).name("query2-selector");

        // Stream partitioned by sea type (Oriental or Occidental)
        KeyedStream<ShipData, String> keyedStream = stream.keyBy(ShipData::getSeaType);

        // Assigning to the stream seven days windows, with calculated offset
        keyedStream.window(TumblingEventTimeWindows.of(Time.days(7), Time.days(ResultsUtils.OFFSET_WEEKLY_TOTAL)))
                    .aggregate(new RankingCellAggregator(), new RankingProcessWindow())
                    .map(new Query2Topology.ResultMapper())
                    .name("query2-weekly-ranking")
                    //.addSink(new MetricsInvoker())
                    .addSink(new FlinkKafkaProducer<>(Configuration.FLINK_QUERY_2_WEEKLY_TOPIC,
                            new FlinkStringToKafkaSerializer(Configuration.FLINK_QUERY_2_WEEKLY_TOPIC),
                            Configuration.getFlinkSinkProperties("producer" +
                                    Configuration.FLINK_QUERY_2_WEEKLY_TOPIC),
                            FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                    .name("query2-weekly-ranking-sink");

        // Assigning to the stream 28 days windows, with calculated offset
        keyedStream.window(TumblingEventTimeWindows.of(Time.days(28), Time.days(ResultsUtils.OFFSET_MONTHLY_TOTAL)))
                .aggregate(new RankingCellAggregator(), new RankingProcessWindow())
                .map(new Query2Topology.ResultMapper())
                .name("query2-monthly-ranking")
                //.addSink(new MetricsInvoker())
                .addSink(new FlinkKafkaProducer<>(Configuration.FLINK_QUERY_2_MONTHLY_TOPIC,
                        new FlinkStringToKafkaSerializer(Configuration.FLINK_QUERY_2_MONTHLY_TOPIC),
                        Configuration.getFlinkSinkProperties("producer" +
                                Configuration.FLINK_QUERY_2_MONTHLY_TOPIC),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query2-monthly-ranking-sink");
    }

    /**
     * Inner class to extract records in such a way to be passed
     * to Kafka Consumer as required.
     */
    private static class ResultMapper implements MapFunction<RankingOutcome, String> {
        @Override
        public String map(RankingOutcome rankingOutcome) {

            StringBuilder builder = new StringBuilder();
            SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
            String date = sdf.format(rankingOutcome.getStartDate());
            builder.append(date);
            builder.append(";");
            builder.append(rankingOutcome.getSeaType());
            builder.append(";");
            builder.append("00:00-11:59");
            builder.append(";");
            builder.append(rankingOutcome.getBeforeRanking());
            builder.append(";");
            builder.append("12:00-23:59");
            builder.append(";");
            builder.append(rankingOutcome.getAfterRanking());
            return builder.toString();
        }
    }
}
