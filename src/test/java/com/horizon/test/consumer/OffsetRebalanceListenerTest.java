package com.horizon.test.consumer;

import com.horizon.mqclient.api.OffsetRebalanceListener;
import com.horizon.mqclient.api.TopicWithPartition;
import com.horizon.mqclient.core.consumer.KafkaHighConsumer;

import java.util.Map;

/**
 * @author : David.Song/Java Engineer
 * @date : 2016/1/19 10:13
 * @see
 * @since : 1.0.0
 */
public class OffsetRebalanceListenerTest {

    public static void main(String[] args){

        KafkaHighConsumer kafkaHighConsumer = KafkaHighConsumer.kafkaHighConsumer();
        String topic = "songwei";
        OffsetListener listener = new OffsetListener();
        kafkaHighConsumer.subscribe(topic,new TestMessageHandler(),listener);
        try {
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            kafkaHighConsumer.close();
        }

    }

    static class OffsetListener extends OffsetRebalanceListener{
        @Override
        public void saveOffsetInExternalStore(Map<TopicWithPartition, Long> tpOffsetMap) {
            System.out.println("saveOffsetInExternalStore==========");
        }

        @Override
        public long readOffsetFromExternalStore(String topic, int partition) {
            return 200;
        }
    }
}
