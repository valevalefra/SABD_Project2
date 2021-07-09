import kafka.SimpleKafkaConsumer;
import config.Configuration;
import utils.queries_utils.ResultsUtils;

import java.util.ArrayList;
import java.util.Scanner;

/**
 * Class used to launch consumers for Flink output
 */
public class ConsumerLauncher {

    public static void main(String[] args) {

        //Clean folder that contain results
        ResultsUtils.cleanFolder();

        ArrayList<SimpleKafkaConsumer> consumers = new ArrayList<>();

        int id = 0;

        for (int i = 0; i < Configuration.LIST_TOPICS.length; i++) {
            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(id,
                    Configuration.LIST_TOPICS[i],
                    ResultsUtils.LIST_CSV[i]);
            consumers.add(consumer);
            new Thread(consumer).start();
            id++;
        }

        System.out.println("\u001B[36m" + "Enter something to stop consumers" + "\u001B[0m");
        Scanner scanner = new Scanner(System.in);
        // wait for the user to digit something
        scanner.next();
        System.out.println("Sending shutdown signal to consumers");
        // stop consumers
        for (SimpleKafkaConsumer consumer : consumers) {
            consumer.stop();
        }
    }
}