package com.sneha.confluent;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class KTableExample {
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
        KTable<String, String> kTable = builder.table(inputTopic, Materialized.with(Serdes.String(), Serdes.String()));
        kTable.mapValues(v->v.toUpperCase())
                .toStream()
                .peek(((key, value) -> System.out.println("After update : key: "+key+" value: "+value)))
                .to(outputTopic,Produced.with(Serdes.String(),Serdes.String()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.start();
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
