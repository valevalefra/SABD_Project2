package utils.metrics;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import utils.metrics.Metrics;

public class MetricsInvoker implements SinkFunction<String> {

    @Override
    public void invoke(String value, Context context) {
        // starts counter
        Metrics.incrementCounter();
    }
}
