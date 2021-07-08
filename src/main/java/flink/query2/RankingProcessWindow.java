package flink.query2;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.Date;

/**
 * Process window to set the right start date and sea type.
 */
public class RankingProcessWindow extends ProcessWindowFunction<RankingOutcome, RankingOutcome, String,
        TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<RankingOutcome> iterable,
                        Collector<RankingOutcome> collector) {
        iterable.forEach(k -> {
            k.setStartDate(new Date(context.window().getStart()));
            k.setSeaType(key);
            collector.collect(k);
        });
    }


}
