package flink.query1;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Process window to set the right start date for the window and
 * to calculate the ships daily average for the obtained result,
 * considering the information relative to the window size given by
 * process context.
 */
public class AverageProcessWindow extends ProcessWindowFunction<AverageOutcome, AverageOutcome, String,
        TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<AverageOutcome> iterable,
                            Collector<AverageOutcome> collector) {
            iterable.forEach(k -> {
                List<Query1Result> listResult = new ArrayList<>();
                Date startDate = new Date(context.window().getStart());
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(startDate);
                long daysBetween = TimeUnit.DAYS.convert(context.window().getEnd() - context.window().getStart(),
                        TimeUnit.MILLISECONDS);
                k.setStartDate(new Date(context.window().getStart()));
                k.setIdCell(key);
                k.getShipMeans().forEach((k1, v) -> {
                    Double avg = (double) v/daysBetween;
                    Query1Result result = new Query1Result();
                    result.switchType(k1, avg, v);
                    listResult.add(result);
                });
                k.setList(listResult);
                collector.collect(k);
            });
        }
    }

