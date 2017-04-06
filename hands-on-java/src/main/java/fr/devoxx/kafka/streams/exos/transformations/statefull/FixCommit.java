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

    private static final String NAME = "FixCommit";
    private static final String APP_ID = AppUtils.appID(NAME);

    public static void main(String[] args) {

        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        StreamsConfig config = new StreamsConfig(AppConfiguration.getProperties(APP_ID));


        final Serde<String> stringSerde = Serdes.String();

        Map<String, Object> serdeProps = new HashMap<>();

        final PojoJsonSerializer<GitMessage> jsonSerializer = new PojoJsonSerializer<>(GitMessage.class.getName());
        serdeProps.put(GitMessage.class.getName(), GitMessage.class);
        jsonSerializer.configure(serdeProps, false);

        final Serde<GitMessage> messageSerde = Serdes.serdeFrom(jsonSerializer, jsonSerializer);

        //START EXO

        KStream<String, GitMessage> commits =
                kStreamBuilder.stream(stringSerde, messageSerde, AppConfiguration.SCALA_GITLOG_TOPIC);

        // the source of the streaming analysis is the topic with git messages
        KTable<String, Long> fixMessageByYears = run(commits, stringSerde);


        //STOP EXO

        fixMessageByYears.print();

        System.out.println("Starting Kafka Streams "+NAME+" Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started  "+NAME+"  Example");

    }

    public static KTable<String, Long> run( KStream<String, GitMessage> commits, Serde<String> stringSerde) {
      // TODO
      return null;
    }

}
