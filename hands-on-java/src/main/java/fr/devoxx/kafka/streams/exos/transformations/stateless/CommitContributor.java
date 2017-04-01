package fr.devoxx.kafka.streams.exos.transformations.stateless;

import fr.devoxx.kafka.streams.pojo.GitMessage;
import fr.devoxx.kafka.streams.pojo.serde.PojoJsonSerializer;
import fr.devoxx.kafka.conf.AppConfiguration;
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
public class CommitContributor {

    private static final String APP_ID = AppUtils.appID("CommitContributor");
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

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        //START EXO

        KStream<String, GitMessage> messagesStream =
                kStreamBuilder.stream(stringSerde, messageSerde, AppConfiguration.SCALA_GITLOG_TOPIC);

        KStream<String, String> commit = messagesStream
                .map((k,v) -> KeyValue.pair(v.getHash(),v.getAuthor()));


        commit.to(stringSerde, stringSerde, "CommitComment");
        //STOP EXO

        System.out.println("Starting Kafka Streams Gitlog Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started Gitlog Example");

    }


}
