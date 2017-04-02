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
public class TotalCommitMessageByUser {


    private static final String NAME = "TotalCommitMessageByUser";
    private static final String APP_ID = AppUtils.appID(NAME);

    public static void main(String[] args) {

        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        StreamsConfig config = new StreamsConfig(AppConfiguration.getProperties(APP_ID));

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Integer> intSerde = Serdes.Integer();

        Map<String, Object> serdeProps = new HashMap<>();

        final PojoJsonSerializer<GitMessage> jsonSerializer = new PojoJsonSerializer<>(GitMessage.class.getName());
        serdeProps.put(GitMessage.class.getName(), GitMessage.class);
        jsonSerializer.configure(serdeProps, false);

        final Serde<GitMessage> messageSerde = Serdes.serdeFrom(jsonSerializer, jsonSerializer);


        KStream<String, GitMessage> scala_gitlog =
                kStreamBuilder.stream(stringSerde, messageSerde, AppConfiguration.SCALA_GITLOG_TOPIC);

        //START EXO
        run(scala_gitlog, stringSerde, intSerde, messageSerde);

        //STOP EXO


        System.out.println("Starting Kafka Streams " + NAME + " Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started  " + NAME + "  Example");
    }

    public static void run(KStream<String, GitMessage> scala_gitlog, Serde<String> stringSerde, Serde<Integer> intSerde, Serde<GitMessage> messageSerde) {
        // the source of the streaming analysis is the topic with git messages

        KTable<String, Integer> aggregate = scala_gitlog
                .groupBy((k, v) -> v.getAuthor(), stringSerde, messageSerde)
                .aggregate(
                        () -> 0,
                        (aggKey, newValue, aggValue) -> {

                            if (newValue.getMessage() != null) {
                                return aggValue + newValue.getMessage().length();

                            } else {
                                return aggValue;
                            }
                        },
                        intSerde,
                        NAME
                );

        aggregate.to(stringSerde, intSerde, NAME);
    }

}
