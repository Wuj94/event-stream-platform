package org.giuseppe.clients;

import Giuseppe.Position;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Assertions;
import org.apache.kafka.streams.KafkaStreams;
import org.giuseppe.VehiclePositionConsumer;
import org.giuseppe.clients.processor.VehiclePositionProcessor;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class TestVehiclePosition {
    static String vehicle_position_stream_out = "vehicle-position-stream-out";
    static String vehicle_position = "vehicle-position";

    private static final VehiclePositionConsumer testConsumer = new VehiclePositionConsumer();

    @Test
    void test_producer_stream() {
        VehiclePositionProducer producer = new VehiclePositionProducer();
        producer.sendRecord(1, new Position(1,2));

        await().atMost(50, TimeUnit.SECONDS).until(() -> testConsumer.nonEmpty(vehicle_position));

        VehiclePositionProcessor processor = new VehiclePositionProcessor();
        KafkaStreams streams = processor.streams();
        streams.cleanUp();
        streams.start();

        await().atMost(50, TimeUnit.SECONDS).until(() -> testConsumer.nonEmpty(vehicle_position_stream_out));
        ConsumerRecords<String, Position> records = testConsumer.retrieveAllStreamOutRecords();
        Position position = records.iterator().next().value();
        streams.close();

        Assertions.assertEquals(position.getX(), 3);
        Assertions.assertEquals(position.getY(), 2);
    }

}

