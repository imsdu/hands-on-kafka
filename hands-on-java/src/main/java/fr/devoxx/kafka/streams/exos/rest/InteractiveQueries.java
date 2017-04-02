package fr.devoxx.kafka.streams.exos.rest;

import fr.devoxx.kafka.conf.AppConfiguration;
import fr.devoxx.kafka.streams.exos.rest.services.InteractiveQueriesRestService;
import fr.devoxx.kafka.streams.exos.transformations.statefull.CountNbrCommitByUser;
import fr.devoxx.kafka.streams.exos.transformations.statefull.FixCommit;
import fr.devoxx.kafka.streams.exos.transformations.statefull.NbrCommitByContributorCategory;
import fr.devoxx.kafka.streams.exos.transformations.statefull.TotalCommitMessageByUser;
import fr.devoxx.kafka.streams.pojo.GitMessage;
import fr.devoxx.kafka.streams.pojo.GithubCommit;
import fr.devoxx.kafka.streams.pojo.serde.PojoJsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class InteractiveQueries {

    //static final String TEXT_LINES_TOPIC = "TextLinesTopic";

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args.length > 2) {
            throw new IllegalArgumentException("usage: ... <portForRestEndPoint> [<bootstrap.servers> (optional)]");
        }
        final int port = Integer.valueOf(args[0]);
        final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";

        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "interactive-queries");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Provide the details of our embedded http service that we'll use to connect to this streams
        // instance and discover locations of stores.
        streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + port);
        final File example = Files.createTempDirectory(new File("/tmp").toPath(), "example").toFile();
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, example.getPath());

        final KafkaStreams streams = createStreams(streamsConfiguration);

        streams.cleanUp();

        streams.start();

        final InteractiveQueriesRestService restService = startRestProxy(streams, port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
                restService.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }


    static InteractiveQueriesRestService startRestProxy(final KafkaStreams streams, final int port)
            throws Exception {
        final InteractiveQueriesRestService
                interactiveQueriesRestService = new InteractiveQueriesRestService(streams);
        interactiveQueriesRestService.start(port);
        return interactiveQueriesRestService;
    }

    static KafkaStreams createStreams(final Properties streamsConfiguration) {
        // Create an instance of StreamsConfig from the Properties instance
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Serde<Integer> intSerde = Serdes.Integer();

        Map<String, Object> serdeProps = new HashMap<>();

        final PojoJsonSerializer<GitMessage> messageSerializer = new PojoJsonSerializer<>(GitMessage.class.getName());
        final PojoJsonSerializer<GithubCommit> commitSerializer = new PojoJsonSerializer<>(GithubCommit.class.getName());
        serdeProps.put(GitMessage.class.getName(), GitMessage.class);
        serdeProps.put(GithubCommit.class.getName(), GithubCommit.class);


        commitSerializer.configure(serdeProps, false);
        messageSerializer.configure(serdeProps, false);

        final Serde<GitMessage> messageSerde = Serdes.serdeFrom(messageSerializer, messageSerializer);
        final Serde<GithubCommit> commitSerde = Serdes.serdeFrom(commitSerializer, commitSerializer);

        KStreamBuilder kStreamBuilder = new KStreamBuilder();


        KStream<String, GithubCommit> commits =
                kStreamBuilder.stream(stringSerde, commitSerde, AppConfiguration.COMMITS_TOPIC);

        KStream<String, GitMessage> scala_gitlog =
                kStreamBuilder.stream(stringSerde, messageSerde, AppConfiguration.SCALA_GITLOG_TOPIC);

        //START EXO

        CountNbrCommitByUser.run(scala_gitlog, stringSerde, longSerde, messageSerde);
        FixCommit.run(scala_gitlog, stringSerde);
        NbrCommitByContributorCategory.run(commits, stringSerde, longSerde, commitSerde);
        TotalCommitMessageByUser.run(scala_gitlog, stringSerde, intSerde, messageSerde);


        //STOP EXO


        return new KafkaStreams(kStreamBuilder, streamsConfiguration);
    }

}