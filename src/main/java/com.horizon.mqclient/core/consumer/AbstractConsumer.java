package com.horizon.mqclient.core.consumer;

import com.horizon.mqclient.api.CommitOffsetCallback;
import com.horizon.mqclient.api.TopicWithPartition;
import com.horizon.mqclient.common.ConsumerStatus;
import com.horizon.mqclient.common.DefaultConsumerConfig;
import com.horizon.mqclient.common.MsgHandleStreamPool;
import com.horizon.mqclient.common.KafkaClientConfig;
import com.horizon.mqclient.exception.KafkaMQException;
import com.horizon.mqclient.serializer.fst.ValueDeserializer;
import com.horizon.mqclient.utils.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

/**
 * @author : David.Song/Java Engineer
 * @date : 2016/1/6 14:33
 * @see
 * @since : 1.0.0
 */
public abstract class AbstractConsumer<K,V> implements Consumer<K,V>{

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    protected KafkaConsumer<K,V> kafkaConsumer;

    protected Map<String,Object> consumerConfigMap = new HashMap<>();

    protected volatile ConsumerStatus status = ConsumerStatus.INIT;

    private boolean isHighLevel = true;

    public AbstractConsumer(boolean isHighLevel){
        this.isHighLevel = isHighLevel;
        //1: init kafka consumer
        initKafkaConsumer();
        //2: init kafka client
        initKafkaClient();
    }

    public AbstractConsumer(boolean isHighLevel,Map<String,Object> consumerConfigMap){
        if(consumerConfigMap == null){
            throw new IllegalArgumentException("consumerConfigMap can`t be null");
        }
        Object enableCommit = consumerConfigMap.get(DefaultConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        if(enableCommit != null && !((Boolean)enableCommit) && isHighLevel){
           throw new IllegalArgumentException("highConsumer enable commit can`t be false!");
        }
        if(enableCommit != null && ((Boolean)enableCommit) && !isHighLevel){
            throw new IllegalArgumentException("lowConsumer enable commit can`t be true!");
        }
        this.isHighLevel = isHighLevel;
        this.consumerConfigMap = consumerConfigMap;
        //1: init kafka consumer
        initKafkaConsumer();
        //2: init kafka client
        initKafkaClient();
    }

    private void initKafkaClient() {

    }

    private void initKafkaConsumer() {
        if(consumerConfigMap.get(DefaultConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) == null){
            if(isHighLevel){
                consumerConfigMap.put(DefaultConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
            }else{
                consumerConfigMap.put(DefaultConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
            }
        }
        if(consumerConfigMap.get(DefaultConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG) == null){
            consumerConfigMap.put(DefaultConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        }
        if(consumerConfigMap.get(DefaultConsumerConfig.FETCH_MIN_BYTES_CONFIG) == null){
            consumerConfigMap.put(DefaultConsumerConfig.FETCH_MIN_BYTES_CONFIG,1);
        }
        consumerConfigMap.put("key.deserializer", StringDeserializer.class);
        consumerConfigMap.put("value.deserializer", ValueDeserializer.class);
        consumerConfigMap.put("bootstrap.servers", KafkaClientConfig.configHolder().getKafkaServers());
        consumerConfigMap.put("group.id", KafkaClientConfig.configHolder().getConsumerGroup());
        kafkaConsumer = new KafkaConsumer(consumerConfigMap);
    }

    @Override
    public void subscribe(List topics){
        if(topics == null || topics.size() <=0 ){
            throw new IllegalArgumentException("topic list can`t be null or empty");
        }
        if (status != ConsumerStatus.INIT) {
            throw new KafkaMQException("The client has been started.");
        }
        this.kafkaConsumer.subscribe(topics);
        status = ConsumerStatus.RUNNING;
    }

    @Override
    public void subscribe(String topic){
        if(StringUtils.isEmpty(topic)){
            throw new IllegalArgumentException("topic can`t be null or empty");
        }
        if (status != ConsumerStatus.INIT) {
            throw new KafkaMQException("The client has been started.");
        }
        this.kafkaConsumer.subscribe(Arrays.asList(topic));
        status = ConsumerStatus.RUNNING;
    }

    @Override
    public void subscribe(Pattern pattern){
        if(pattern == null){
            throw new IllegalArgumentException("pattern can`t be null");
        }
        if (status != ConsumerStatus.INIT) {
            throw new KafkaMQException("The client has been started.");
        }
        this.kafkaConsumer.subscribe(pattern, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                logger.info("partitions revoked!");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                logger.info("partitions assigned!");
            }
        });
        status = ConsumerStatus.RUNNING;
    }

    @Override
    public void assign(TopicWithPartition topicWithPartition)  {
        if(topicWithPartition == null){
            throw new IllegalArgumentException("topicWithPartition cant be null");
        }
        if (status != ConsumerStatus.INIT) {
            throw new KafkaMQException("The client has been started.");
        }
        TopicPartition singleTopicPartition = new TopicPartition(topicWithPartition.getTopic(),
                topicWithPartition.getPartition());
        this.kafkaConsumer.assign(Arrays.asList(singleTopicPartition));
        status = ConsumerStatus.RUNNING;
    }

    @Override
    public void assign(TopicWithPartition... topicWithPartitions)  {
        if(topicWithPartitions == null || topicWithPartitions.length == 0){
            throw new IllegalArgumentException("topicWithPartitions can`t be null or empty");
        }
        if (status != ConsumerStatus.INIT) {
            throw new KafkaMQException("The client has been started.");
        }
        List<TopicPartition>  topicPartitionList = new ArrayList<>();
        for(TopicWithPartition topicWithPartition : topicWithPartitions){
            TopicPartition topicPartition = new TopicPartition(topicWithPartition.getTopic(),
                    topicWithPartition.getPartition());
            topicPartitionList.add(topicPartition);
        }
        this.kafkaConsumer.assign(topicPartitionList);
        status = ConsumerStatus.RUNNING;
    }

    @Override
    public void assign(String topic, Integer[] partitions)  {
        if(StringUtils.isEmpty(topic) || partitions == null || partitions.length <0){
            throw new IllegalArgumentException("topic can`t be null and partitions length >=0");
        }
        if (status != ConsumerStatus.INIT) {
            throw new KafkaMQException("The client has been started.");
        }
        List<TopicPartition>  topicPartitionList = new ArrayList<>();
        for(Integer partition : partitions){
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            topicPartitionList.add(topicPartition);
        }
        this.kafkaConsumer.assign(topicPartitionList);
        status = ConsumerStatus.RUNNING;
    }


    @Override
    public void resetOffset(TopicWithPartition topicWithPartition, long resetOffset)  {
        if(topicWithPartition == null){
            throw new IllegalArgumentException("topicWithPartitions can`t be null");
        }
        if(status != ConsumerStatus.RUNNING){
            throw new KafkaMQException("The consumer is not running now!");
        }
        TopicPartition topicPartition = new TopicPartition(topicWithPartition.getTopic(),
                topicWithPartition.getPartition());
        this.kafkaConsumer.seek(topicPartition, resetOffset);
    }


    @Override
    public void resetOffsetToBegin(TopicWithPartition... topicWithPartitions)  {
        if(topicWithPartitions == null || topicWithPartitions.length == 0){
            throw new IllegalArgumentException("topicWithPartitions can`t be null or contain no elements");
        }
        if(status != ConsumerStatus.RUNNING){
            throw new KafkaMQException("The consumer is not running now!");
        }
        TopicPartition[] tmpTpArray = convertTopicPartition(topicWithPartitions);
        this.kafkaConsumer.seekToBeginning(tmpTpArray);
    }


    @Override
    public void resetOffsetToEnd(TopicWithPartition... topicWithPartitions)  {
        if(topicWithPartitions == null || topicWithPartitions.length == 0){
            throw new IllegalArgumentException("topicWithPartitions can`t be null or contain no elements");
        }
        if(status != ConsumerStatus.RUNNING){
            throw new KafkaMQException("The consumer is not running now!");
        }
        TopicPartition[] tmpTpArray = convertTopicPartition(topicWithPartitions);
        this.kafkaConsumer.seekToEnd(tmpTpArray);
    }

    @Override
    public long currentOffset(TopicWithPartition topicWithPartition)  {
        if(topicWithPartition == null){
            throw new IllegalArgumentException("topicWithPartition can`t be null");
        }
        if(status != ConsumerStatus.RUNNING){
            throw new KafkaMQException("The consumer is not running now!");
        }
        TopicPartition topicPartition = new TopicPartition(topicWithPartition.getTopic(),
                topicWithPartition.getPartition());
        return this.kafkaConsumer.position(topicPartition);
    }


    @Override
    public void pauseConsume(TopicWithPartition... topicWithPartitions)  {
        if(topicWithPartitions == null || topicWithPartitions.length ==0){
            throw new IllegalArgumentException("topicWithPartitions can`t be null " +
                    " and or empty");
        }
        if(status != ConsumerStatus.RUNNING){
            throw new KafkaMQException("The consumer is not running now!");
        }
        TopicPartition[] tmpTpArray = convertTopicPartition(topicWithPartitions);
        this.kafkaConsumer.pause(tmpTpArray);
    }

    @Override
    public void resumeConsume(TopicWithPartition... topicWithPartitions)  {
        if(topicWithPartitions == null || topicWithPartitions.length ==0){
            throw new IllegalArgumentException("topicWithPartitions can`t be null " +
                    " and or empty");
        }
        if(status != ConsumerStatus.RUNNING){
            throw new KafkaMQException("The consumer is not running now!");
        }
        TopicPartition[] tmpTpArray = convertTopicPartition(topicWithPartitions);
        this.kafkaConsumer.resume(tmpTpArray);
    }

    @Override
    public void commitAsync()  {
    }

    @Override
    public void commitAsync(Map<TopicWithPartition,Long> offsets, CommitOffsetCallback callback)  {
    }

    @Override
    public void commitAsync(CommitOffsetCallback callback)  {
    }

    @Override
    public void commitSync()  {
    }

    @Override
    public void commitSync(Map<TopicWithPartition, Long> offsets)  {
    }

    @Override
    public void close() {
        status = ConsumerStatus.STOPPING;
        logger.info("Client Consumer is stopping.......");
        MsgHandleStreamPool.poolHolder().turnOffPool();
        try {
            if (!MsgHandleStreamPool.poolHolder().waitUntilAllTaskFinished()) {
                MsgHandleStreamPool.poolHolder().turnOffPoolNow(); // Cancel currently executing tasks
                if (!MsgHandleStreamPool.poolHolder().waitUntilAllTaskFinished())
                    logger.error("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            MsgHandleStreamPool.poolHolder().turnOffPoolNow();
        }
        kafkaConsumer.unsubscribe();
        kafkaConsumer.close();
        logger.info("Client Consumer is closed!");
        status = ConsumerStatus.STOPPED;
    }

    protected TopicPartition[] convertTopicPartition(TopicWithPartition[] topicWithPartitions) {
        TopicPartition[] tmpTpArray = new TopicPartition[topicWithPartitions.length];
        int k = 0;
        for(TopicWithPartition topicWithPartition : topicWithPartitions){
            TopicPartition topicPartition = new TopicPartition(topicWithPartition.getTopic(),
                    topicWithPartition.getPartition());
            tmpTpArray[k] = topicPartition;
            k++;
        }
        return tmpTpArray;
    }
}
