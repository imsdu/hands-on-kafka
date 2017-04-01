package fr.devoxx.kafka.streams.conf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * Created by fred on 01/04/2017.
 */
public  class AppConfiguration {

    public static String URL_BASE = "192.168.99.100"; //"localhost"
    public static String BOOTSTRAP_SERVERS_CONFIG = URL_BASE+ ":9092";
    public static String SCHEMA_REGISTRY =  "http://"+URL_BASE+ ":8081";
    public static String STATE_DIR_CONFIG = "/tmp";
    public static String AUTO_OFFSET_RESET_CONFIG = "earliest";


    public static Properties getProperties(String APP_ID) {
        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        // Kafka bootstrap server (broker to talk to)
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfiguration.BOOTSTRAP_SERVERS_CONFIG);

        settings.put(StreamsConfig.STATE_DIR_CONFIG, AppConfiguration.STATE_DIR_CONFIG);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  AppConfiguration.AUTO_OFFSET_RESET_CONFIG);
        return settings;
    }
}
