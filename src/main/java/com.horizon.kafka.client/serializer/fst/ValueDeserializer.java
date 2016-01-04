package com.horizon.kafka.client.serializer.fst;

import com.horizon.kafka.client.core.Message;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * 实现kafka Deserializer接口的Fst反序列化
 */
public class ValueDeserializer implements Deserializer<Message> {

    private FstSerializer fstSerializer;

    /**
     * 初始化配置
     * @param configs 配置信息
     * @param isKey 是key还是value
     */
    public void configure(Map<String, ?> configs, boolean isKey) {
        fstSerializer = new FstSerializer();
    }

    /**
     * 反序列化方法
     * @param topic 主题名字
     * @param data 字节数组
     * @return 事件对象
     * @throws SerializationException 序列化异常
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
