package kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import config.Configuration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleKafkaConsumer implements Runnable {

    private int id;
    private String topic;
    private String output;
    private final static String CONSUMER_GROUP_ID = "topics-consumer";
    private final Consumer<String, String> consumer;
    private boolean running = true;

    /**
     * Create a new consumer using the properties
     *
     * @return the created consumer
     */
    private Consumer<String, String> createConsumer() {
        Properties props = Configuration.getKafkaParametricConsumerProperties(CONSUMER_GROUP_ID);
        return new KafkaConsumer<>(props);
    }

    /**
     * Subscribe a consumer to a topic
     *
     * @param consumer to be subscribe
     * @param topic    chosen
     */
    private static void subscribeToTopic(Consumer<String, String> consumer, String topic) {
        consumer.subscribe(Collections.singletonList(topic));
    }

    public SimpleKafkaConsumer(int id, String topic, String output) {
        this.id = id;
        this.topic = topic;
        this.output = output;

        // create the consumer
        consumer = createConsumer();

        // subscribe the consumer to the topic
        subscribeToTopic(consumer, topic);

    }

    @Override
    public void run() {

        System.out.println("Consumer " + id + " running...");
        try {
            while (running) {
                Thread.sleep(2000);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                System.out.println(!records.isEmpty());
                if (!records.isEmpty()) {
                    File file = new File(output);
                    if (!file.exists()) {
                        // creates the file if it does not exist
                        file.createNewFile();
                    }

                    // append to existing version of the same file
                    FileWriter writer = new FileWriter(file, true);
                    BufferedWriter bw = new BufferedWriter(writer);
                    for (ConsumerRecord<String, String> record : records) {
                        bw.append(record.value());
                        System.out.println(record.value());
                        System.out.println("\n");
                        bw.append("\n");
                    }

                    // close both buffered writer and file writer
                    bw.close();
                    writer.close();
                }
            }

        } catch (InterruptedException ignored) {
            // ignored
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // close consumer
            consumer.close();
        }

    }

    public void stop() {
        this.running = false;
    }

}
