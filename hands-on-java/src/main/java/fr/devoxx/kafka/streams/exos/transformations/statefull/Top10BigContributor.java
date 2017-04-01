package fr.devoxx.kafka.exos.transformations.statefull;

import fr.devoxx.kafka.streams.pojo.GitMessage;
import fr.devoxx.kafka.streams.pojo.serde.PojoJsonSerializer;
import fr.devoxx.kafka.conf.AppConfiguration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.HashMap;
import java.util.Map;

/**
 * Hands-on kafka streams Devoxx 2017
 */
public class Top10BigContributor {

    private static final String APP_ID = "hands-on-kafka-java-app";

    public static void main(String[] args) {

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(AppConfiguration.getProperties(APP_ID));
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        Map<String, Object> serdeProps = new HashMap<>();

        final PojoJsonSerializer<GitMessage> jsonSerializer = new PojoJsonSerializer<>();
        serdeProps.put("PojoJsonSerializer", GitMessage.class);
        jsonSerializer.configure(serdeProps, false);

        final Serde<GitMessage> messageSerde = Serdes.serdeFrom(jsonSerializer, jsonSerializer);

        // building Kafka Streams Model
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        // the source of the streaming analysis is the topic with git messages
        KStream<String, GitMessage> messagesStream =
                kStreamBuilder.stream(stringSerde, messageSerde, "scala-gitlog");

        KTable<String, Long> messagesPerUser = messagesStream
                .groupBy((key, message) -> message.getAuthor(), stringSerde, messageSerde)
                .count("CountPerUser");
        messagesPerUser.to(stringSerde, longSerde, "MessagesPerUser");
        //messagesPerUser.print(stringSerde, longSerde);

        System.out.println("Starting Kafka Streams Gitlog Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started Gitlog Example");

    }


}
