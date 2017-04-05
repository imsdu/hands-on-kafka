package fr.devoxx.kafka.streams.exos.store;

import fr.devoxx.kafka.conf.AppConfiguration;
import fr.devoxx.kafka.streams.pojo.GithubCommit;
import fr.devoxx.kafka.streams.pojo.GithubUser;
import fr.devoxx.kafka.streams.pojo.serde.PojoJsonSerializer;
import fr.devoxx.kafka.utils.AppUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Hands-on kafka streams Devoxx 2017
 */
public class LocalKVStore {

    private static final String APP_ID = AppUtils.appID("LocalKV");

    public static void main(String[] args) {

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(AppConfiguration.getProperties(APP_ID));
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // building Kafka Streams Model
        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        Map<String, Object> serdeProps = new HashMap<>();

        // Commits
        final PojoJsonSerializer<GithubCommit> commitSerializer = new PojoJsonSerializer<>("GithubCommit");
        serdeProps.put("GithubCommit", GithubCommit.class);
        commitSerializer.configure(serdeProps, false);

        final Serde<GithubCommit> commitSerde = Serdes.serdeFrom(commitSerializer, commitSerializer);

        final KStream<String, GithubCommit> commitStream = kStreamBuilder.stream(stringSerde, commitSerde, "commits");

      final KTable<String, Long> countCommitsByAuthor = run(stringSerde, commitSerde, commitStream);
      countCommitsByAuthor.print();

        System.out.println("Starting Kafka Streams Local KV Store Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started Local KV Store Example");
    }

    public static KTable<String, Long> run(Serde<String> stringSerde, Serde<GithubCommit> commitSerde, KStream<String, GithubCommit> commitStream) {
      final KStream<String, GithubCommit> commitsByLogin = commitStream
              .filter((key, commit) -> commit != null && commit.getAuthor() != null)
              .map((key, commit) -> KeyValue.pair(commit.getAuthor().getLogin(), commit));


      final KGroupedStream<String, GithubCommit> groupedCommitsByAuthor  = commitsByLogin.groupByKey(stringSerde,commitSerde);
      return groupedCommitsByAuthor.count("CountCommitsByAuthor");
    }

}
