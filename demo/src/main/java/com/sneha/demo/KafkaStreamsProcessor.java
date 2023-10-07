package com.sneha.demo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

/**

 * HOW TO RUN THIS :
 * 1) Start Zookeeper and Kafka. 
 * 2) Create the input and output topics used by this example.
 * $ bin/kafka-topics.sh --create --topic TextLinesTopic \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics.sh --create --topic UppercasedTextLinesTopic \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics.sh --create --topic OriginalAndUppercasedTopic \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * 3) Start this example application either in your IDE or on the command line.
 * 4) Write some input data to the source topic (e.g. via {@code kafka-console-producer}). The already
 * running example application (step 3) will automatically process this input data and write the
 * results to the output topics.
 * # Start the console producer.  You can then enter input data by writing some line of text, followed by ENTER:
 * #
 * #   hello kafka streams<ENTER>
 * #   all streams lead to kafka<ENTER>
 * #
 * # Every line you enter will become the value of a single Kafka message.
 * $ bin/kafka-console-producer --broker-list localhost:9092 --topic TextLinesTopic
 * }</pre>
 * 5) Inspect the resulting data in the output topics, e.g. via {@code kafka-console-consumer}.
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer --topic UppercasedTextLinesTopic --from-beginning \
 *                              --bootstrap-server localhost:9092
 * $ bin/kafka-console-consumer --topic OriginalAndUppercasedTopic --from-beginning \
 *                              --bootstrap-server localhost:9092 --property print.key=true
 * }</pre>
 * You should see output data similar to:
 * <pre>
 * {@code
 * HELLO KAFKA STREAMS
 * ALL STREAMS LEAD TO KAFKA
 * }</pre>
 * 6) Once you're done with your experiments, you can stop this example via { Ctrl-C}.  If needed,
 * also stop the Kafka broker ({ Ctrl-C}), and only then stop the ZooKeeper instance ({@code Ctrl-C}).
 */
public class KafkaStreamsProcessor {

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "map-function-example-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Set up serializers and deserializers, which we will use for overriding the default serdes
        // specified above.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<byte[]> byteArraySerde = Serdes.ByteArray();

        // In the subsequent lines we define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();

        // Read the input Kafka topic into a KStream instance.
        final KStream<byte[], String> textLines = builder.stream("TextLinesTopic", Consumed.with(byteArraySerde, stringSerde));

        // Variant 1: using `mapValues`
        textLines.mapValues(v -> v.toUpperCase()).to("UppercasedTextLinesTopic");

        // Variant 2: using `map`, modify value only (equivalent to variant 1)
        final KStream<String, String> originalAndUppercased = textLines.map((key, value) -> KeyValue.pair(value, value.toUpperCase()));
        originalAndUppercased.to("OriginalAndUppercasedTopic", Produced.with(stringSerde, stringSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}