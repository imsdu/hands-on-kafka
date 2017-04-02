package fr.devoxx.kafka.streams.exos.transformations.statefull;

import fr.devoxx.kafka.conf.AppConfiguration;
import fr.devoxx.kafka.streams.pojo.GithubCommit;
import fr.devoxx.kafka.streams.pojo.serde.PojoJsonSerializer;
import fr.devoxx.kafka.utils.AppUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.HashMap;
import java.util.Map;

/**
 * Hands-on kafka streams Devoxx 2017
 */
public class NbrCommitByContributorCategory {
    private static final String NAME = "NbrCommitByContributorCategory";
    private static final String APP_ID = AppUtils.appID(NAME);

    public static void main(String[] args) {

        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        StreamsConfig config = new StreamsConfig(AppConfiguration.getProperties(APP_ID));

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        Map<String, Object> serdeProps = new HashMap<>();

        final PojoJsonSerializer<GithubCommit> jsonSerializer = new PojoJsonSerializer<>(GithubCommit.class.getName());
        serdeProps.put(GithubCommit.class.getName(), GithubCommit.class);
        jsonSerializer.configure(serdeProps, false);

        final Serde<GithubCommit> commitSerde = Serdes.serdeFrom(jsonSerializer, jsonSerializer);


        KStream<String, GithubCommit> commits =
                kStreamBuilder.stream(stringSerde, commitSerde, AppConfiguration.COMMITS_TOPIC);


        //START EXO

        run(commits, stringSerde, longSerde, commitSerde);
        //STOP EXO


        System.out.println("Starting Kafka Streams " + NAME + " Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started  " + NAME + "  Example");

    }

    public static void run(KStream<String, GithubCommit> commits, Serde<String> stringSerde, Serde<Long> longSerde, Serde<GithubCommit> commitSerde) {

        KTable<String, Long> commit = commits
                .selectKey((k, v) -> v.getSha())
                .map((k, v) -> {
                    String category = "OTHER";
                    String email = v.getCommit().getAuthor().getEmail();
                    if (email.contains("@lightbend.com"))
                        category = "LIGHTBEND";
                    else if (email.contains("@epfl.ch"))
                        category = "EPFL";

                    return KeyValue.pair(category, v);
                })
                .groupByKey(stringSerde,commitSerde)
                .count(NAME);


        commit.to(stringSerde, longSerde, NAME);
    }


}
