package com.paulo.kafka;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

    public static void main(String[] args) {
        // CREATE PROPERTIES
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka=streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // CREATE TOPOLOGY
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // INPUT TOPIC
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStreams = inputTopic.filter(
                (k, jsonTweet) -> extractUserFollowers(jsonTweet) > 10000
        );
        filteredStreams.to("important_tweets");

        // BUILD TOPOLOGY
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // START STREAM APPLICATION
        kafkaStreams.start();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static Integer extractUserFollowers(String tweetJson) {
        try {
            return jsonParser.parse(tweetJson).getAsJsonObject()
                    .get("user").getAsJsonObject()
                    .get("followers_count").getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }

    }

}
