package com.github.apaceh;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class LocationProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(LocationProducer.class);

        // define circle location
        RandomLatLngUtils.Solution solution = new RandomLatLngUtils.Solution(10, 20, 30);

        String bootstrapServers = "172.18.46.11:9092,172.18.46.12:9092,172.18.46.13:9092";
        String topic = "users-location";

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            List coordinate = solution.randPoint();

            int id = i + 1;
            String value = new JSONObject()
                    .put("id", 1)
                    .put("user_id", id)
                    .put("lat", coordinate.getItem(0))
                    .put("lng", coordinate.getItem(1))
                    .toString();

            String key = "id_" + id;

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            logger.info("Key: " + key);

            // send data
            producer.send(producerRecord, (recordMetadata, e) -> {
                // executes every time a record is successfully sent or an exception is thrown
                if (e == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing.", e);
                }
            }).get(); // block the .send to make it synchronous
        }

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}



