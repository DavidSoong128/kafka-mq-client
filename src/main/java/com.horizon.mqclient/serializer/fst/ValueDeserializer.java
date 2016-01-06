package com.horizon.mqclient.serializer.fst;

import com.horizon.mqclient.api.Message;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * mq message value deserializer tool
 */
public class ValueDeserializer implements Deserializer<Message> {

    private FstSerializer fstSerializer;
    /**
     */
    public void configure(Map<String, ?> configs, boolean isKey) {
        fstSerializer = new FstSerializer();
    }

    public Message deserialize(String topic, byte[] data) {
        try {
            return fstSerializer.deserialize(data,Message.class);
        } catch (IOException e) {
            throw new SerializationException("fst deserializer error",e);
        }
    }
    @Override
    public void close() {
    }
}
