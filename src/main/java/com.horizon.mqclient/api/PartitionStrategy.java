package com.horizon.mqclient.api;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * producer partition strategy , class should extends PartitionStrategy
 * @author : David.Song/Java Engineer
 * @date : 2016/1/7 4:01
 * @see
 * @since : 1.0.0
 */
public abstract class PartitionStrategy implements Partitioner{
    /**
     * producer can override the partition method ,
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (keyBytes == null) {
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() > 0) {
                return this.computePartition(topic,null, availablePartitions.size());
            } else {
                // no partitions are available, give a non-available partition
                return this.computePartition(topic, null, numPartitions);
            }
        } else {
            // hash the keyBytes to choose a partition
            return this.computePartition(topic, key, numPartitions);
        }
    }
    /**
     * override the method
     * @param topic : current partition of the topic
     * @param  partitionKey : key to partition
     * @param numPartitions : current topic total partition number
     * @return
     */
    protected abstract int computePartition(String topic, Object partitionKey, int numPartitions);


    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
