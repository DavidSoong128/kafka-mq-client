package com.horizon.mqclient.serializer.fst;

import com.horizon.mqclient.core.Message;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * ʵ��kafka Deserializer�ӿڵ�Fst�����л�
 */
public class ValueDeserializer implements Deserializer<Message> {

    private FstSerializer fstSerializer;

    /**
     * ��ʼ������
     * @param configs ������Ϣ
     * @param isKey ��key����value
     */
    public void configure(Map<String, ?> configs, boolean isKey) {
        fstSerializer = new FstSerializer();
    }

    /**
     * �����л�����
     * @param topic ��������
     * @param data �ֽ�����
     * @return �¼�����
     * @throws SerializationException ���л��쳣
     */
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
