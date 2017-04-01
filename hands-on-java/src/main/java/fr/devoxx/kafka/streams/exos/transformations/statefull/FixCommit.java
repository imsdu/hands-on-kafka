package fr.devoxx.kafka.streams.exos.transformations.statefull;

import fr.devoxx.kafka.conf.AppConfiguration;
import fr.devoxx.kafka.streams.pojo.GitMessage;
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
public class FixCommit {

    private static final String APP_ID = AppUtils.appID("FixCommit");

    public static void main(String[] args) {

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(AppConfiguration.getProperties(APP_ID));
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        Map<String, Object> serdeProps = new HashMap<>();

        final PojoJsonSerializer<GitMessage> jsonSerializer = new PojoJsonSerializer<>();
        serdeProps.put("PojoJsonSerializer", GitMessage.class);
        jsonSerializer.configure(serdeProps, false);

        final Serde<GitMessage> messageSerde = Serdes.serdeFrom(jsonSerializer, jsonSerializer);

        //START EXO


        // building Kafka Streams Model
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        // the source of the streaming analysis is the topic with git messages
        KStream<String, GitMessage> messagesStream =
                kStreamBuilder.stream(stringSerde, messageSerde, "scala-gitlog");

        KTable<String, Long> fixMessageByYears = messagesStream
                .map((k,v) -> KeyValue.pair(v.getDate().split("-")[0] , v.getMessage()))
                .filter((k,v) -> v != null && v.toLowerCase().contains(" fix "))
                .groupByKey(stringSerde,stringSerde)
                .count("countFix");


        //STOP EXO

        fixMessageByYears.print();


        System.out.println("Starting Kafka Streams Gitlog Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started Gitlog Example");

    }

}
