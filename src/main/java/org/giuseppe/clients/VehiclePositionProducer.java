package org.giuseppe.clients;

import Giuseppe.Position;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class VehiclePositionProducer {
    public static Properties settings = new Properties();
    private final Producer<String, Position> producer;

    public VehiclePositionProducer() {
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"kafka:9092");
        settings.put(ProducerConfig.ACKS_CONFIG,"all");
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer .class);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        settings.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
        producer = new KafkaProducer<>(settings);
    }

    public void sendRecord(int key, Position position) {
        ProducerRecord<String, Position> producer_record = new ProducerRecord<>("vehicle-position", Integer.toString(key), position);
        producer.send(producer_record, null);
        producer.close();
    }
}