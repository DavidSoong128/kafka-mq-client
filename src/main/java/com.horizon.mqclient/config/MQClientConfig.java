package com.horizon.mqclient.config;

import java.io.InputStream;
import java.util.Properties;

/**
 * Config handle class
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 11:53
 * @since : 1.0.0
 */
public class MQClientConfig {

    private final static String		URL							        = "/kafka-client.properties";

    private final static String BOOTSTRAP_SERVERS_KEY = "kafka.server.hosts";

    private final static String CONSUMER_GROUP_KEY = "consumer.group";

    private MQClientConfig() {
    }

    private static class Holder {
        private static MQClientConfig	config	= new MQClientConfig();
    }

    public static MQClientConfig getInstence() {
        return Holder.config;
    }

    private Properties prop	= getProperties(URL);

    private static Properties getProperties(String url) {
        try {
            Properties prop = new Properties();
            InputStream in = MQClientConfig.class.getResourceAsStream(url);
            prop.load(in);
            in.close();
            return prop;
        } catch (Exception e) {
        }
        return null;
    }

    public String getKakfaServers(){
        return prop.getProperty(BOOTSTRAP_SERVERS_KEY);
    }

    public String getConsumerGroup(){
        return prop.getProperty(CONSUMER_GROUP_KEY);
    }
}
