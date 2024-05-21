package org.example;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Arrays;
import java.util.Properties;
public class KafkaQueue {
    private final String topic;
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    public KafkaQueue(String topic) {
        this.topic = topic;
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<String, String>(producerProps);
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "queue-consumer-group");
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumer = new KafkaConsumer<String, String>(consumerProps);
        consumer.subscribe(Arrays.asList(topic));
    }
    public void enqueue(String message) {
        producer.send(new ProducerRecord<String, String>(topic, message));
    }
    public String dequeue() {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        if (!records.isEmpty()) {
            String message = records.iterator().next().value();
            return message;
        }
        return null;
    }
    public void close() {
        producer.close();
        consumer.close();
    }

    public static void main(String[] args) {
        String topic = "my_queue";
        KafkaQueue queue = new KafkaQueue(topic);

        // Enqueue messages
        queue.enqueue("Message 1");
        queue.enqueue("Message 2");

        // Dequeue messages
        System.out.println("Dequeued message: " + queue.dequeue());
        System.out.println("Dequeued message: " + queue.dequeue());

        queue.close();
    }
}
