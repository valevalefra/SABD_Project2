package config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class Configuration {

   // topics
   public static final String FLINK_TOPIC = "flink-topic";
   public static final String FLINK_QUERY_1_WEEKLY_TOPIC = "flink-output-topic-query1-weekly";
   public static final String FLINK_QUERY_1_MONTHLY_TOPIC = "flink-output-topic-query1-monthly";
   public static final String FLINK_QUERY_2_WEEKLY_TOPIC = "flink-output-topic-query2-weekly";
   public static final String FLINK_QUERY_2_MONTHLY_TOPIC = "flink-output-topic-query2-monthly";
    public static final String FLINK_QUERY_3_1HOUR_TOPIC = "flink-output-topic-query3-1hour";
    public static final String FLINK_QUERY_3_2HOUR_TOPIC = "flink-output-topic-query3-2hour";

    // brokers
    public static final String KAFKA_BROKER_1 = "localhost:9092";
    public static final String KAFKA_BROKER_2 = "localhost:9093";
    public static final String KAFKA_BROKER_3 = "localhost:9094";

    // bootstrap servers
    public static final String BOOTSTRAP_SERVERS = KAFKA_BROKER_1 + "," + KAFKA_BROKER_2 + "," + KAFKA_BROKER_3;

    // if consumer has no offset for the queue, starts from the first record
    private static final String CONSUMER_FIRST_OFFSET = "earliest";
    private static final String ENABLE_CONSUMER_EXACTLY_ONCE = "read_committed";

    //semantic type
    private static final boolean ENABLE_PRODUCER_EXACTLY_ONCE = true;

    public static final String[] LIST_TOPICS = {FLINK_QUERY_1_WEEKLY_TOPIC, FLINK_QUERY_1_MONTHLY_TOPIC,
            FLINK_QUERY_2_WEEKLY_TOPIC, FLINK_QUERY_2_MONTHLY_TOPIC, FLINK_QUERY_3_1HOUR_TOPIC, FLINK_QUERY_3_2HOUR_TOPIC};



    /**
     * Creates properties for a Kafka Consumer representing the Flink stream source
     * @param consumerGroupId id of consumer group
     * @return created properties
     */
    public static Properties getFlinkSourceProperties(String consumerGroupId) {
        Properties props = new Properties();

        // specify brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // set consumer group id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        // start reading from beginning of partition if no offset was created
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUMER_FIRST_OFFSET);
        // exactly once semantic
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ENABLE_CONSUMER_EXACTLY_ONCE);

        // key and value deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return props;
    }

    public static Properties getFlinkSinkProperties(String producerId) {
        Properties props = new Properties();

        // specify brokers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // set producer id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
        // exactly once semantic
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_PRODUCER_EXACTLY_ONCE);

        return props;
    }

    public static Properties getKafkaParametricConsumerProperties(String consumerGroupId) {
        Properties props = new Properties();

        // specify brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS);
        // set consumer group id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        // exactly once semantic
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ENABLE_CONSUMER_EXACTLY_ONCE);

        // key and value deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return props;
    }

}
