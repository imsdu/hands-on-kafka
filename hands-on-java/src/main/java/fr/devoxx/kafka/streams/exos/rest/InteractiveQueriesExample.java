package fr.devoxx.kafka.streams.exos.rest;

import fr.devoxx.kafka.conf.AppConfiguration;
import fr.devoxx.kafka.streams.exos.rest.services.InteractiveQueriesRestService;
import fr.devoxx.kafka.streams.exos.transformations.statefull.CountNbrCommitByUser;
import fr.devoxx.kafka.streams.pojo.GitMessage;
import fr.devoxx.kafka.streams.pojo.serde.PojoJsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class InteractiveQueriesExample {

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
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "interactive-queries-example");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Provide the details of our embedded http service that we'll use to connect to this streams
        // instance and discover locations of stores.
        streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + port);
        final File example = Files.createTempDirectory(new File("/tmp").toPath(), "example").toFile();
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, example.getPath());

        final KafkaStreams streams = createStreams(streamsConfiguration);
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();
        // Now that we have finished the definition of the processing topology we can actually run
        // it via `start()`.  The Streams application as a whole can be launched just like any
        // normal Java application that has a `main()` method.
        streams.start();

        // Start the Restful proxy for servicing remote access to state stores
        final InteractiveQueriesRestService restService = startRestProxy(streams, port);

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
                restService.stop();
            } catch (Exception e) {
                // ignored
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
                .count(CountNbrCommitByUser.NAME);

        messagesPerUser.to(stringSerde, longSerde, CountNbrCommitByUser.NAME);

        //STOP EXO


        return new KafkaStreams(kStreamBuilder, streamsConfiguration);
    }

}