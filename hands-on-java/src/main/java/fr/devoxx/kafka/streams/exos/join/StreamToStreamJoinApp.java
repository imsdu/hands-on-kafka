package fr.devoxx.kafka.streams.exos.join;

import fr.devoxx.kafka.conf.AppConfiguration;
import fr.devoxx.kafka.streams.pojo.GitMessage;
import fr.devoxx.kafka.streams.pojo.GithubCommit;
import fr.devoxx.kafka.streams.pojo.GithubUser;
import fr.devoxx.kafka.streams.pojo.serde.PojoJsonSerializer;
import fr.devoxx.kafka.utils.AppUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Hands-on kafka streams Devoxx 2017
 */
public class StreamToStreamJoinApp {

    private static final String APP_ID = AppUtils.appID("StreamToStreamJoin");

    public static void main(String[] args) {

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(AppConfiguration.getProperties(APP_ID));
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // building Kafka Streams Model
        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        Map<String, Object> serdeProps = new HashMap<>();

        // Users
        final PojoJsonSerializer<GithubUser> userSerializer = new PojoJsonSerializer<>("GithubUser");
        serdeProps.put("GithubUser", GithubUser.class);
        userSerializer.configure(serdeProps, false);

        final Serde<GithubUser> userSerde = Serdes.serdeFrom(userSerializer, userSerializer);
        final KStream<String, GithubUser> userStream = kStreamBuilder.stream(stringSerde, userSerde, "users", "all-users");

        // Commits
        final PojoJsonSerializer<GithubCommit> commitSerializer = new PojoJsonSerializer<>("GithubCommit");
        serdeProps.put("GithubCommit", GithubCommit.class);
        commitSerializer.configure(serdeProps, false);

        final Serde<GithubCommit> commitSerde = Serdes.serdeFrom(commitSerializer, commitSerializer);

        final KStream<String, GithubCommit> commitStream = kStreamBuilder.stream(stringSerde, commitSerde, "commits");

        // Join !
      run(stringSerde, longSerde, userSerde, userStream, commitSerde, commitStream);

        System.out.println("Starting Kafka Streams Joins Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started Gitlog Example");
    }

    public static void run(Serde<String> stringSerde, Serde<Long> longSerde, Serde<GithubUser> userSerde, KStream<String, GithubUser> userStream, Serde<GithubCommit> commitSerde, KStream<String, GithubCommit> commitStream) {
      final KStream<String, GithubUser> userByLogin = userStream.map((key, user) -> KeyValue.pair(user.getLogin(), user));
      final KStream<String, GithubCommit> commitsByLogin = commitStream
              .filter((key, commit) -> commit != null && commit.getAuthor() != null)
              .map((key, commit) -> KeyValue.pair(commit.getAuthor().getLogin(), commit));

      final KStream<String, Long> userCommits = userByLogin.leftJoin(commitsByLogin,
              (user, commit) -> { if(commit == null) { return 0L; } else { return 1L; } },
              JoinWindows.of(TimeUnit.SECONDS.toMillis(60)),
              stringSerde, userSerde, commitSerde
      );

      final KTable<String, Long> nbCommitsByUser = userCommits.groupByKey(stringSerde, longSerde).reduce((v1, v2) -> v1 + v2, "CommitSum");

      nbCommitsByUser.to(stringSerde, longSerde,"NbCommitsPerUserTopic");

      nbCommitsByUser.print();
    }
}
