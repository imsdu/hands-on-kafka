package fr.devoxx.kafka.streams.exos.join;

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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.HashMap;
import java.util.Map;

/**
 * Hands-on kafka streams Devoxx 2017
 */
public class TableToTableJoinApp {

    private static final String APP_ID = AppUtils.appID("TableToTableJoin");

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
        final KTable<String, GithubUser> userTable = kStreamBuilder.stream(stringSerde, userSerde, "users", "all-users")
                .groupBy((key, user) -> user.getLogin(), stringSerde, userSerde)
                .reduce((v1, v2) -> v1, "UserTable");

        // Commits
        final PojoJsonSerializer<GithubCommit> commitSerializer = new PojoJsonSerializer<>("GithubCommit");
        serdeProps.put("GithubCommit", GithubCommit.class);
        commitSerializer.configure(serdeProps, false);

        final Serde<GithubCommit> commitSerde = Serdes.serdeFrom(commitSerializer, commitSerializer);

        final KTable<String, GithubCommit> lastCommitByUser = kStreamBuilder.stream(stringSerde, commitSerde, "commits")
                .filter((key, commit) -> commit != null && commit.getAuthor() != null)
                .groupBy((key, commit) -> commit.getAuthor().getLogin(), stringSerde, commitSerde)
                .reduce((v1, v2) -> v2, "LastCommitTable");

        // Join !
        final KTable<String, String> lastCommits = lastCommitByUser.join(userTable,
                (commit, user) -> user.getLogin() + " " + user.getEmail() + " " + commit.getSha() + " " + commit.getCommit().getMessage()
        );

        lastCommits.print();

        System.out.println("Starting Kafka Streams Joins Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started Gitlog Example");
    }
}
