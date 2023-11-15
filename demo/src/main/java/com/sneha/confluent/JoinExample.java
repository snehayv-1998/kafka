package com.sneha.confluent;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;

public class JoinExample {
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
        System.out.println("********************************************************* joins started");

        StreamsBuilder builder = new StreamsBuilder();
        final String topicA = streamsProps.getProperty("basic.input.topic");
        final String topicB = streamsProps.getProperty("basic.output.topic");
        KStream<String, String> leftStream = builder.stream(topicA);
        KStream<String, String> rightStream = builder.stream(topicB);
        leftStream.peek(((key, value) -> System.out.println("************************* leftStream : key: "+key+" value: "+value)));
        rightStream.peek(((key, value) -> System.out.println("************************ rightStream : key: "+key+" value: "+value)));

        ValueJoiner<String, String, String> valueJoiner = (leftValue, rightValue) -> leftValue + rightValue;
        leftStream.join(rightStream,valueJoiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(50)))
                .peek(((key, value) -> System.out.println("After join : key: "+key+" value: "+value)));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
        streams.start();
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
