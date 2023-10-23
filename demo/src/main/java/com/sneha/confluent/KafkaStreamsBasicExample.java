package com.sneha.confluent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class KafkaStreamsBasicExample {

    public static void main(final String[] args) throws IOException {
      /*  if (args.length != 1) {
            System.out.println("Please provide the configuration file path as a command line argument");
            System.exit(1);
        }*/

        // Load producer configuration settings from a local file
        final Properties props = loadConfig("src\\main\\resources\\static\\client.properties");
        streamProcessor(props);
    }

    private static void streamProcessor(Properties streamsProps) throws IOException {

        //Use the utility method TopicLoader.runProducer() to create the required topics on the cluster and produce some sample records
        TopicLoader.runProducer();

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("basic.input.topic");
        final String outputTopic = streamsProps.getProperty("basic.output.topic");
        KStream<String, String> firstStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
        firstStream.peek((key, value) -> System.out.println("Incoming record - key " +key +" value " + value))
                .mapValues(value -> value.toUpperCase())
                .peek((key, value) -> System.out.println("Updated record - key " +key +" value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.start();
        System.out.println("kafkaStreams.metadataForAllStreamsClients() :"+ kafkaStreams.metadataForAllStreamsClients());
        System.out.println("kafkaStreams.allMetadata()) :"+kafkaStreams.allMetadata());
    }
    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
}