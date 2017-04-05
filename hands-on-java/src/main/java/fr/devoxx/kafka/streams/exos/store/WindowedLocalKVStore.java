package fr.devoxx.kafka.streams.exos.store;

import fr.devoxx.kafka.conf.AppConfiguration;
import fr.devoxx.kafka.streams.pojo.GithubCommit;
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

/**
 * Hands-on kafka streams Devoxx 2017
 */
public class WindowedLocalKVStore {

  private static final String APP_ID = AppUtils.appID("WindowedKV");
  public static final String NAME = "CountWindowedCommitsByAuthor";
  public static final long MS = 1000;

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

        final KTable<Windowed<String>, Long> countCommitsByAuthor = run(stringSerde, longSerde, commitStream);

        countCommitsByAuthor.print();

        System.out.println("Starting Kafka Streams Windowed KV Store Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started Windowed KV Store Example");
    }

  public static KTable<Windowed<String>, Long> run(Serde<String> stringSerde, Serde<Long> longSerde, KStream<String, GithubCommit> commitStream) {
    final KStream<String, Long> commitsByLogin = commitStream
            .filter((key, commit) -> commit != null && commit.getAuthor() != null)
            .map((key, commit) -> KeyValue.pair(commit.getAuthor().getLogin(), 1L));
    
    final KGroupedStream<String, Long> groupedCommitsByAuthor  = commitsByLogin.groupBy((key, val) -> key, stringSerde, longSerde);

    return groupedCommitsByAuthor.count(TimeWindows.of(MS), NAME);
  }
}
