package com.horizon.mqclient.common;

import java.io.InputStream;
import java.util.Properties;

/**
 * Config handle class
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 11:53
 * @since : 1.0.0
 */
public class KafkaClientConfig {

    private final static String CONFIG_PATH = "/kafka-client.properties";

    private final static String BOOTSTRAP_SERVERS_KEY = "kafka.server.hosts";

    private final static String CONSUMER_GROUP_KEY = "consumer.group";

    private KafkaClientConfig() {
    }

    private static class Holder {
        private static KafkaClientConfig config	= new KafkaClientConfig();
    }

    public static KafkaClientConfig configHolder() {
        return Holder.config;
    }

    private Properties prop	= getProperties(CONFIG_PATH);

    private static Properties getProperties(String url) {
        try {
            Properties prop = new Properties();
            InputStream in = KafkaClientConfig.class.getResourceAsStream(url);
            prop.load(in);
            in.close();
            return prop;
        } catch (Exception e) {
        }
        return null;
    }

    public String getKafkaServers(){
        String kafkaServerHosts = prop.getProperty(BOOTSTRAP_SERVERS_KEY);
        if(kafkaServerHosts == null || kafkaServerHosts.length() ==0)
            throw new IllegalArgumentException("kafka.server.hosts can`t be null");
        return kafkaServerHosts;
    }

    public String getConsumerGroup(){
        String groupId = prop.getProperty(CONSUMER_GROUP_KEY);
        if(groupId == null || groupId.length() ==0)
            throw new IllegalArgumentException("consumer.group can`t be null");
        return groupId;
    }
}
