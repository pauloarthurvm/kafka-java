package com.kafkatwitter.paulo;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class TwitterProducer {

    String consumerKey = "5OP7mDzmd9kzaKOmvxwLY5dO4";
    String consumerSecret = "IZvavBbOvBcnt99OqnUDoePnKZuRY6WR0AQagYuI9pjCgAWHCW";
    String token = "1454795516269588492-p6UdOyAkkg3wJO81l1M4ZFZFdwHivu";
    String secret = "Iq475SLl8PCtgydOr0wRMIgvRcfKCrNONH2OsCfiOxJsz";

    String bootstrapServers = "127.0.0.1:9092";

    List<String> terms = Lists.newArrayList("bitcoin");

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public static void main(String[] args) {
        System.out.println("Hello twitter");
        new TwitterProducer().run();
    }

    public void run() {
        logger.info("Setup");

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

//        Create a twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();

//        Create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

//        Add a shutdown hooker
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            logger.info("Shutting down client from twitter...");
            client.stop();
            logger.info("Closing producer...");
            producer.close();
            logger.info("Done.");
        }));

//        Loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }
        }
        logger.info("End of app.");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth =
                new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer() {
//        Producer properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        Safe producer
        properties.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ACKS_CONFIG, "all");
        properties.setProperty(RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

//        High throughput producer
        properties.setProperty(COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(LINGER_MS_CONFIG, "20");
        properties.setProperty(BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));


//        Create producer
        return new KafkaProducer<String, String>(properties);

    }

}