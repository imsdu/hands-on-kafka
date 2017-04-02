package fr.devoxx.kafka.streams.exos.transformations.stateless;

import fr.devoxx.kafka.conf.AppConfiguration;
import fr.devoxx.kafka.streams.pojo.GithubCommit;
import fr.devoxx.kafka.streams.pojo.serde.PojoJsonSerializer;
import fr.devoxx.kafka.utils.AppUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Hands-on kafka streams Devoxx 2017
 */
public class ContributorNoLightbend {


    private static final String NAME = "ContributorNoLightbend";

    private static final String APP_ID = AppUtils.appID(NAME);


    public static void main(String[] args) {

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(AppConfiguration.getProperties(APP_ID));
        final Serde<String> stringSerde = Serdes.String();

        Map<String, Object> serdeProps = new HashMap<>();

        final PojoJsonSerializer<GithubCommit> jsonSerializer = new PojoJsonSerializer<>();
        serdeProps.put(PojoJsonSerializer.POJO_JSON_SERIALIZER, GithubCommit.class);
        jsonSerializer.configure(serdeProps, false);

        final Serde<GithubCommit> commitSerde = Serdes.serdeFrom(jsonSerializer, jsonSerializer);

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        //START EXO

        KStream<String, GithubCommit> messagesStream =
                kStreamBuilder.stream(stringSerde, commitSerde, AppConfiguration.COMMITS_TOPIC);

        KStream<String, GithubCommit> commit = messagesStream
                .selectKey((k,v )-> v.getSha())
                .filterNot((k,v) -> v.getCommit().getAuthor().getEmail().contains("@lightbend.com"));



        commit.to(stringSerde, commitSerde, NAME);
        //STOP EXO


        System.out.println("Starting Kafka Streams "+NAME+" Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started  "+NAME+"  Example");
    }


}
