package com.horizon.kafka.client.core.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

import java.util.List;
import java.util.regex.Pattern;

/**
 * <pre>
 *     abstract method of kafka consumer
 * </pre>
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 15:59
 * @see
 * @since : 1.0.0
 */
public interface Consumer<K,V> {
    /**
     * subscribe one topic
     */
    public void subscribe(String topic);

    /**
     * subscribe list topics
     */
    public void subscribe(List<String> topics);

    /**
     * subscribe list topics with callback
     */
    public void subscribe(List<String> topics, ConsumerRebalanceListener callback);

    /**
     * subscribe pattern topic with callback
     */
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback);
}
