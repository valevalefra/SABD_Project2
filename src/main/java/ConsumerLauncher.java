import kafka.SimpleKafkaConsumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import utils.KafkaConfig;
import utils.OutputUtils;

import java.security.cert.TrustAnchor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

/**
 * Class used to launch consumers for Flink output
 */
public class ConsumerLauncher {

    public static void main(String[] args) {

        //Clean folder that contain results
        OutputUtils.cleanFolder();

        ArrayList<SimpleKafkaConsumer> consumers = new ArrayList<>();

        int id = 0;

        for (int i = 0; i < KafkaConfig.LIST_TOPICS.length; i++) {
            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(id,
                    KafkaConfig.LIST_TOPICS[i],
                    OutputUtils.LIST_CSV[i]);
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