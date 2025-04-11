package com.aamir.stream.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class kafkaConfig {


    @Bean
    public KafkaStreams kafkaStreams() {
        // Kafka Streams configuration properties
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-consumer-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Using StreamsBuilder instead of KStreamBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // Consuming messages from the "stream-topic" and modifying them
        KStream<String, String> stream = builder.stream("stream-topic");

        // Modifying the message and sending it to another topic "processed-topic"
        stream.mapValues(value -> value.concat(addMessage(value)))
                .to("test-stream-topic");

        // Creating the KafkaStreams instance and starting it
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        return streams;
    }

    private String addMessage(String input) {
        return switch (input.toLowerCase().replaceAll("^\"(.*)\"$", "$1")) {
            case "hi" -> ", How are you?";
            case "hello" -> ", Good Morning";
            default -> "Default message added";
        };
    }
}
