package kafka;

import config.Configuration;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleKafkaProducer {

    private final static String PRODUCER_ID = "simple-producer";

    private String topic;

    private Producer<Long, String> producer;

    public SimpleKafkaProducer(String topic) {

        this.topic = topic;
        producer = createProducer();

    }

    private static Producer<Long, String> createProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    public void produce(Long key, String value) {


        final ProducerRecord<Long, String> record = new ProducerRecord<>("flink-topic", null,
                value);

        producer.send(record);

        // DEBUG
           /* System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
                    record.key(), record.value(), metadata.partition(), metadata.offset());*/

    }

    public void close() {
        producer.flush();
        producer.close();
    }
}