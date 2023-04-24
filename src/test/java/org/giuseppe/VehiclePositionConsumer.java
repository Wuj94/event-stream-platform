package org.giuseppe;

import java.time.Duration;
import java.util.*;

import Giuseppe.Position;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;


public class VehiclePositionConsumer {
    private final KafkaConsumer<String, Position> consumer;
    private final KafkaConsumer<String, Position> streamConsumer;

    public VehiclePositionConsumer() {
        Properties settings = new Properties();
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, String.format("consumer-%s", UUID.randomUUID()));
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        settings.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://schema-registry:8081");
        settings.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        consumer = new KafkaConsumer<>(settings);
        consumer.subscribe(List.of("vehicle-position"));

        settings.put(ConsumerConfig.GROUP_ID_CONFIG, String.format("consumer-%s", UUID.randomUUID()));
        streamConsumer = new KafkaConsumer<>(settings);
        streamConsumer.subscribe(List.of("vehicle-position-stream-out"));
    }

    public Boolean nonEmpty(String topic) {
        return switch (topic) {
            case "vehicle-position" -> nonEmptyVehiclePosition();
            case "vehicle-position-stream-out" -> nonEmptyVehiclePositionStreamOut();
            default -> throw new IllegalStateException("Unexpected value: " + topic);
        };
    }

    private Boolean nonEmptyVehiclePosition() {
        consumer.seekToBeginning(List.of());
        Iterator<ConsumerRecord<String, Position>> records = consumer.poll(Duration.ofMillis(20000)).records("vehicle-position").iterator();
        return records.hasNext();
    }

    private Boolean nonEmptyVehiclePositionStreamOut() {
        consumer.seekToBeginning(List.of());
        Iterator<ConsumerRecord<String, Position>> records = streamConsumer.poll(Duration.ofMillis(20000)).records("vehicle-position-stream-out").iterator();
        return records.hasNext();
    }

    public ConsumerRecords<String, Position> retrieveAllStreamOutRecords() {
        streamConsumer.seekToBeginning(List.of());
        return streamConsumer.poll(Duration.ofMillis(20000));
    }
}