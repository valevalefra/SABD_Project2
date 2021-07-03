package flink.query2;

import flink.query1.AverageOutcome;
import flink.query1.Query1Result;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RankingProcessWindow extends ProcessWindowFunction<RankingOutcome, RankingOutcome, String,
        TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<RankingOutcome> iterable,
                        Collector<RankingOutcome> collector) {
        iterable.forEach(k -> {
            k.setStartDate(new Date(context.window().getStart()));
            k.setSeaType(key);
            //System.out.println("chiave " + key);
            collector.collect(k);
        });
    }


}
