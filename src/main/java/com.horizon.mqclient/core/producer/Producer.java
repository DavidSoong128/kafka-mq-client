package com.horizon.mqclient.core.producer;

import com.horizon.mqclient.api.Message;
import com.horizon.mqclient.api.ProducerSendCallback;
import com.horizon.mqclient.api.TopicWithPartition;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *   kafka client produce message to broker
 * </pre>
 * @author : David.Song/Java Engineer
 * @date : 2016/1/13 16:40
 * @see
 * @since : 1.0.0
 */
public interface Producer<K,V> {
    /**
     * Send the given record asynchronously and return a future which will eventually contain the response information.
     * @param message The record to send
     * @param topic The topic to send
     * @return A future which will eventually contain the response information
     */
    public Map<TopicWithPartition,Long> send(String topic, Message message);

    /**
     * send message given record and topic and partition
     * @param topic
     * @param partition
     * @param message
     * @return
     */
    public Map<TopicWithPartition,Long> send(String topic, Integer partition, Message message);
    /**
     * invoke callback after send message
     * @param topic
     * @param message
     * @param callback
     * @return
     */
    public Map<TopicWithPartition,Long> send(String topic, Message message, ProducerSendCallback callback);
    /**
     * send message given record and topic and partition
     * invoke callback after send message
     * @param topic
     * @param partition
     * @param message
     * @return
     */
    public Map<TopicWithPartition,Long> send(String topic, Integer partition, Message message,
                                             ProducerSendCallback callback);
    /**
     * Flush any accumulated records from the producer. Blocks until all sends are complete.
     */
    public void flush();

    /**
     * Close this producer
     */
    public void close();

    /**
     * Tries to close the producer cleanly within the specified timeout. If the close does not complete within the
     * timeout, fail any pending send requests and force close the producer.
     */
    public void close(long timeout, TimeUnit unit);
}
