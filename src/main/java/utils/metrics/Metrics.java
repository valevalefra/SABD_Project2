package utils.metrics;

public class Metrics {

    // tuples counter
    private static long counter = 0L;
    // first output time
    private static long startTime;

    /**
     * Called when a new observation is seen, updates statistics
     */
    public static synchronized void incrementCounter() {
        if (counter == 0L) {
            startTime = System.currentTimeMillis();
        }
        counter++;
        double currentTime = System.currentTimeMillis() - startTime;

        // prints mean throughput and latency so far evaluated
        System.out.println("Mean throughput: " + (counter/currentTime) + "\n" + "Mean latency: " +
                (currentTime/counter));
    }
}
