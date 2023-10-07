package com.sneha.demo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class KafkaTableProcessor {
    public static void main(String[] args) {
        // Configure Kafka Streams application
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-example");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Create a Kafka Streams builder
        StreamsBuilder builder = new StreamsBuilder();

        // Create a KTable from a topic
        KTable<String, String> kTable = builder.table(
                "input-topic",
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as(Stores.inMemoryKeyValueStore("my-ktable-store"))
        );
        // Perform operations on the KTable, such as filtering, aggregation, joins, etc.

        // Start the Kafka Streams application
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
