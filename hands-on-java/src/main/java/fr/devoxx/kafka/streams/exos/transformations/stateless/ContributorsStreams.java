package fr.devoxx.kafka.streams.exos.transformations.stateless;

import fr.devoxx.kafka.conf.AppConfiguration;
import fr.devoxx.kafka.streams.pojo.Contributor;
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

import java.util.HashMap;
import java.util.Map;

/**
 * Hands-on kafka streams Devoxx 2017
 */
public class ContributorsStreams {


    private static final String NAME = "ContributorsStreams";
    private static final String APP_ID = AppUtils.appID(NAME);

    public static void main(String[] args) {

        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        StreamsConfig config = new StreamsConfig(AppConfiguration.getProperties(APP_ID));

        final Serde<Integer> integerSerde = Serdes.Integer();
        final Serde<String> stringSerde = Serdes.String();

        Map<String, Object> serdeProps = new HashMap<>();

        final PojoJsonSerializer<Contributor> jsonSerializer = new PojoJsonSerializer<>(Contributor.class.getName());
        serdeProps.put(Contributor.class.getName(), Contributor.class);
        jsonSerializer.configure(serdeProps, false);

        final Serde<Contributor> contributorSerde = Serdes.serdeFrom(jsonSerializer, jsonSerializer);


        //START EXO

        run(kStreamBuilder, integerSerde, stringSerde, contributorSerde);
        //STOP EXO

        System.out.println("Starting Kafka Streams "+NAME+" Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println("Now started  "+NAME+"  Example");

    }

    public static void run(KStreamBuilder kStreamBuilder, Serde<Integer> integerSerde, Serde<String> stringSerde, Serde<Contributor> contributorSerde) {
      // TDO
    }


}
