package kafka;

import config.Configuration;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleKafkaProducer {

    private final static String PRODUCER_ID = "simple-producer";

    private String topic;

    private Producer<Long, String> producer;

    public SimpleKafkaProducer(String topic) {

        this.topic = topic;
        producer = createProducer();

    }

    /**
     * Create a new kafka producer
     * @return the created kafka producer
     */
    private static Producer<Long, String> createProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    /**
     * Function that publish a message to both the flink's and kafka streams' topic
     * @param value line to be send
     */
    public void produce(String value) {

        final ProducerRecord<Long, String> record = new ProducerRecord<>("flink-topic", null,
                value);

        producer.send(record);

    }

    public void close() {
        producer.flush();
        producer.close();
    }
}