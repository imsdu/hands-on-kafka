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
public class FixStreams {


    private static final String NAME = "FixStreams";
    private static final String APP_ID = AppUtils.appID(NAME);

    public static void main(String[] args) {

        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        StreamsConfig config = new StreamsConfig(AppConfiguration.getProperties(APP_ID));

        final Serde<String> stringSerde = Serdes.String();

        Map<String, Object> serdeProps = new HashMap<>();

        final PojoJsonSerializer<GithubCommit> jsonSerializer = new PojoJsonSerializer<>(GithubCommit.class.getName());
        serdeProps.put(GithubCommit.class.getName(), GithubCommit.class);
        jsonSerializer.configure(serdeProps, false);

        final Serde<GithubCommit> githubCommitSerde = Serdes.serdeFrom(jsonSerializer, jsonSerializer);

        KStream<String, GithubCommit> contributorStream =
                kStreamBuilder.stream(stringSerde, githubCommitSerde, AppConfiguration.COMMITS_TOPIC);

        //START EXO


        run(contributorStream,  stringSerde, githubCommitSerde);
        //STOP EXO

        System.out.println("Starting Kafka Streams "+NAME+" Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started  "+NAME+"  Example");

    }

    public static void run( KStream<String, GithubCommit> contributorStream, Serde<String> stringSerde, Serde<GithubCommit> githubCommitSerde) {


        KStream<String, GithubCommit> commit = contributorStream
                .selectKey((k, v) -> v.getSha())
        .filter((k,v)-> v.getCommit().getMessage().toLowerCase().contains("fix "));

        commit.print();
        commit.to(stringSerde, githubCommitSerde, NAME);
    }


}
