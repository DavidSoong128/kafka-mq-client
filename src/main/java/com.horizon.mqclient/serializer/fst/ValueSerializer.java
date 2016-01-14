package com.horizon.mqclient.serializer.fst;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * kafka message value serializer tool
 * @author : David.Song/Java Engineer
 * @date : 2016/1/14 10:45
 * @see
 * @since : 1.0.0
 */
public class ValueSerializer implements Serializer{

    private FstSerializer fstSerializer;

    @Override
    public void configure(Map configs, boolean isKey) {
        fstSerializer = new FstSerializer();
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        try {
            return fstSerializer.serialize(data);
        } catch (Exception e) {
            throw new SerializationException("fst serializer error",e);
        }
    }

    @Override
    public void close() {
    }
}
