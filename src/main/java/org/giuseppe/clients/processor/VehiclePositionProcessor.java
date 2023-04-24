package org.giuseppe.clients.processor;

import Giuseppe.Position;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class VehiclePositionProcessor {
    private static final Properties config = new Properties();
    private final StreamsBuilder builder = new StreamsBuilder();

    public VehiclePositionProcessor() {
        try(Serde<String> stringSerde = Serdes.String()) {
            config.put(StreamsConfig.APPLICATION_ID_CONFIG, "vehicle-position-processor");
            config.put(StreamsConfig.CLIENT_ID_CONFIG, "vehicle-position-client");
            config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass());
            config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
            config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
            config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1);
            config.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 30000);
            config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
            config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    "http://schema-registry:8081");
        }
    }

    public KafkaStreams streams() {
        try(final SpecificAvroSerde<Position> poisitionSerde = new SpecificAvroSerde<>()) {
            final Map<String, String> serdeConfig =
                    Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            "http://schema-registry:8081");
            poisitionSerde.configure(serdeConfig, false);
            config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, poisitionSerde.getClass());

            builder.<String, Position>stream("vehicle-position")
                    .mapValues((value) -> new Position(value.getX() + 2, value.getY()))
                    .to("vehicle-position-stream-out");
            return new KafkaStreams(builder.build(), config);
        }
    }
}
