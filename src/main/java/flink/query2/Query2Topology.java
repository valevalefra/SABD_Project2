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
import utils.FlinkStringToKafkaSerializer;
import utils.KafkaConfig;
import utils.MonthlyWindowAssigner;
import utils.ShipData;

import java.text.SimpleDateFormat;

public class Query2Topology {

    public static void buildTopology(DataStream<Tuple2<Long, String>> source) {

        DataStream<ShipData> stream = source
                .flatMap(new FlatMapFunction<Tuple2<Long, String>, ShipData>() {
                    @Override
                    public void flatMap(Tuple2<Long, String> tuple, Collector<ShipData> collector) {
                        ShipData data;
                        String[] info = tuple._2().split(",");

                        data = new ShipData(Double.parseDouble(info[3]), Double.parseDouble(info[4]), tuple._1(), info[0]);
                        collector.collect(data);
                    }
                });

        KeyedStream<ShipData, String> keyedStream = stream.keyBy(ShipData::getSeaType);
        keyedStream.window(TumblingEventTimeWindows.of(Time.days(7)))
                    .aggregate(new RankingCellAggregator(), new RankingProcessWindow())
                    .map(new Query2Topology.ResultMapper())
                    .addSink(new FlinkKafkaProducer<>(KafkaConfig.FLINK_QUERY_2_WEEKLY_TOPIC,
                            new FlinkStringToKafkaSerializer(KafkaConfig.FLINK_QUERY_2_WEEKLY_TOPIC),
                            KafkaConfig.getFlinkSinkProperties("producer" +
                                    KafkaConfig.FLINK_QUERY_2_WEEKLY_TOPIC),
                            FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                    .name("query2-weekly-sink");

        keyedStream.window(new MonthlyWindowAssigner())
                .aggregate(new RankingCellAggregator(), new RankingProcessWindow())
                .map(new Query2Topology.ResultMapper())
                .addSink(new FlinkKafkaProducer<>(KafkaConfig.FLINK_QUERY_2_MONTHLY_TOPIC,
                        new FlinkStringToKafkaSerializer(KafkaConfig.FLINK_QUERY_2_MONTHLY_TOPIC),
                        KafkaConfig.getFlinkSinkProperties("producer" +
                                KafkaConfig.FLINK_QUERY_2_MONTHLY_TOPIC),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query2-monthly-sink");
    }

    private static class ResultMapper implements MapFunction<RankingOutcome, String> {
        @Override
        public String map(RankingOutcome rankingOutcome) throws Exception {

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
