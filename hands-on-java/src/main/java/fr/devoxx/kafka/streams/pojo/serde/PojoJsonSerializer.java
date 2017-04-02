package fr.devoxx.kafka.streams.pojo.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by sdumas on 18/03/17.
 */
public class PojoJsonSerializer<T> implements Serializer<T>, Deserializer<T> {

    public String key;
    private ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> tClass;


    public PojoJsonSerializer(String key)
    {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.key = key;
    }


   // public static String POJO_JSON_SERIALIZER = "PojoJsonSerializer";

    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes == null)
            return null;
        T data;
        try {
            data = objectMapper.readValue(bytes, tClass);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void configure(Map<String, ?> props, boolean b) {
        tClass = (Class<T>) props.get(key);
    }

    @Override
    public byte[] serialize(String data, T t) {

        if (data == null)
            return null;
        try {
            return objectMapper.writeValueAsBytes(t);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {

    }
}
