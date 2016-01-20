package com.horizon.mqclient.core.consumer;

import com.horizon.mqclient.api.CommitOffsetCallback;
import com.horizon.mqclient.api.MessageHandler;
import com.horizon.mqclient.api.OffsetRebalanceListener;
import com.horizon.mqclient.api.TopicWithPartition;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

import java.util.List;
import java.util.Map;
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
     * ==================================subscribe topic===========================================
     */
    /**
     * subscribe one topic
     */
    public void subscribe(String topic, MessageHandler handler);

    /**
     * subscribe one topic
     * This is applicable when the consumer is having Kafka auto-manage group membership.
     */
    public void subscribe(String topic, MessageHandler handler, OffsetRebalanceListener listener);

    /**
     * subscribe list topics
     * This is applicable when the consumer is having Kafka auto-manage group membership.
     */
    public void subscribe(List<String> topics, MessageHandler handler, OffsetRebalanceListener listener);

    /**
     * subscribe list topics
     */
    public void subscribe(List<String> topics,MessageHandler handler);

    /**
     * subscribe pattern topic with callback
     */
    public void subscribe(Pattern pattern, MessageHandler handler, OffsetRebalanceListener listener);

    /**
     * subscribe pattern topic with callback
     */
    public void subscribe(Pattern pattern,MessageHandler handler);

    /**
     * close client consumer,release resources
     */
    public void close();


    /**
     * ===================================assign topic with partition=========================
     */


    /**
     * Manually assign a list of partition to this consumer
     * This interface does not allow for incremental assignment
     * and will replace the previous assignment (if there is one).
     * <p>
     * Manual topic assignment through this method does not use the consumer's group management
     * functionality. As such, there will be no rebalance operation triggered when group membership or cluster and topic
     * metadata change. Note that it is not possible to use both manual partition assignment with {@link #assign(List)}
     * and group assignment with {@link #subscribe(List, ConsumerRebalanceListener)}.
     *
     * @param topicWithPartition
     *
     */
    public void assign(TopicWithPartition topicWithPartition,MessageHandler handler);


    /**
     * assgin topic list to a list of partition
     * @param topicWithPartitions
     */
    public void assign(TopicWithPartition[] topicWithPartitions,MessageHandler handler);

    /**
     * one topic to list of partition
     * @param topic
     * @param partitions
     * @throws Exception
     */
    public void assign(String topic, Integer[] partitions,MessageHandler handler);




    /**
     * ====================================reset partition offset========================================
     */



    /**
     * reset the offset to resumes message
     */
    public void resetOffset(TopicWithPartition topicWithPartition, long resetOffset);

    /**
     * reset the offset to begin offset
     * @param topicWithPartitions
     */
    public void resetOffsetToBegin(TopicWithPartition... topicWithPartitions);


    /**
     * reset the offset to end offset
     * @param topicWithPartitions
     */
    public void resetOffsetToEnd(TopicWithPartition... topicWithPartitions);


    /**
     * get the topic with partition offset
     * @param topicWithPartition
     * @return
     * @throws Exception
     */
    public long currentOffset(TopicWithPartition topicWithPartition);


    /**
     *
     * ==================================Consumption Flow Control================================
     */

    /**
     * Kafka supports dynamic controlling of consumption flows by using pause and resume to pause the consumption
     * on the specified assigned partitions and resume the consumption on the specified paused partitions
     * respectively in the future poll(long) calls.
     */
    public void pauseConsume(TopicWithPartition... topicWithPartitions);


    public void resumeConsume(TopicWithPartition... topicWithPartitions);


    /**
     *
     * ==================================commit offset operation================================
     */
    /**
     * Commit offsets returned on the last poll() for all the subscribed list of topics and partition.
     * manual commit offset with not blocking
     * @throws Exception
     */
    public void commitAsync();

    /**
     * Commit the specified offsets for the specified list of topics and partitions to Kafka
     * @param offsets
     * @param callback
     */
    public void commitAsync(Map<TopicWithPartition,Long> offsets, CommitOffsetCallback callback);

    /**
     * Commit offsets returned on the last poll() for the subscribed list of topics and partitions.
     * @param callback
     * @throws Exception
     */
    public void commitAsync(CommitOffsetCallback callback);


    /**
     * Commit offsets returned on the last poll() for all the subscribed list of topics and partitions.
     * @throws Exception
     */
    public void commitSync();

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     * @param offsets
     * @throws Exception
     */
    public void commitSync(Map<TopicWithPartition,Long> offsets);

}
