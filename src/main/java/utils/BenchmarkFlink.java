package utils;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class BenchmarkFlink  implements SinkFunction<String> {

    @Override
    public void invoke(String value, Context context) {
        // stats counter
        SynchronizedCounter.incrementCounter();
    }
}
