package fr.devoxx.kafka.producer;

/**
 * Created by fred on 01/04/2017.
 */

import fr.devoxx.kafka.streams.conf.AppConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

public class InitProducer {
    public static void main(String[] args){
        if (args.length != 2) {
            System.out.println("Please provide command line arguments: filePath topic");
            System.exit(-1);
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", AppConfiguration.BOOTSTRAP_SERVERS_CONFIG);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", AppConfiguration.SCHEMA_REGISTRY);


        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props);
        String fileName = args[0];
        String topic = args[1];

        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {

            stream.forEach(line ->  {

            ProducerRecord<String, String> data = new ProducerRecord<>(topic, line);
            producer.send(data);

            });

        } catch (IOException e) {
            e.printStackTrace();
        }

        producer.close();
    }
}