package flink.query3;

import flink.query2.RankingOutcome;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class DistanceProcessWindow extends ProcessAllWindowFunction<DistanceOutcome, DistanceOutcome,
        TimeWindow> {


    @Override
    public void process(Context context, Iterable<DistanceOutcome> iterable, Collector<DistanceOutcome> collector) throws Exception {

        iterable.forEach(k -> {
            k.setStartDate(new Date(context.window().getStart()));
           // System.out.println(k.getStartDate());
            collector.collect(k);
        });

    }
}


