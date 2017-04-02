package fr.devoxx.kafka.streams.exos.transformations.statefull;

import fr.devoxx.kafka.conf.AppConfiguration;
import fr.devoxx.kafka.streams.pojo.GitMessage;
import fr.devoxx.kafka.streams.pojo.serde.PojoJsonSerializer;
import fr.devoxx.kafka.utils.AppUtils;
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
public class CountNbrCommitByUser {

    private static final String  NAME = "CountNbrCommitByUser";
    private static final String APP_ID = AppUtils.appID(NAME);

    public static void main(String[] args) {

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(AppConfiguration.getProperties(APP_ID));
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        Map<String, Object> serdeProps = new HashMap<>();

        final PojoJsonSerializer<GitMessage> jsonSerializer = new PojoJsonSerializer<>();
        serdeProps.put(PojoJsonSerializer.POJO_JSON_SERIALIZER, GitMessage.class);
        jsonSerializer.configure(serdeProps, false);

        final Serde<GitMessage> messageSerde = Serdes.serdeFrom(jsonSerializer, jsonSerializer);

        //START EXO

        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        KStream<String, GitMessage> messagesStream =
                kStreamBuilder.stream(stringSerde, messageSerde, AppConfiguration.SCALA_GITLOG_TOPIC);

        KTable<String, Long> messagesPerUser = messagesStream
                .groupBy((key, message) ->
                        message.getAuthor(), stringSerde, messageSerde)
                .count("CountPerUser");

        messagesPerUser.to(stringSerde, longSerde, NAME);

        //STOP EXO



        messagesPerUser.print(stringSerde, longSerde);


        System.out.println("Starting Kafka Streams "+NAME+" Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started  "+NAME+"  Example");

    }


}
