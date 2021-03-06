package utils.kafka_utils;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class FlinkStringToKafkaSerializer implements KafkaSerializationSchema<String> {
    // topic where the string will be published to
    private final String topic;

    public FlinkStringToKafkaSerializer(String topic) {
        super();
        this.topic = topic;
    }

    /**
     * Converts the string to a Kafka-ready record
     * @param value string to publish
     * @return record ready to be published to Kafka
     */
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String value, @Nullable Long aLong) {
        return new ProducerRecord<>(topic, value.getBytes(StandardCharsets.UTF_8));
    }
}