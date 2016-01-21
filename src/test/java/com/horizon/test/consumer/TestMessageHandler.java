package com.horizon.test.consumer;

import com.horizon.mqclient.api.ConsumerResult;
import com.horizon.mqclient.api.Message;
import com.horizon.mqclient.api.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : David.Song/Java Engineer
 * @date : 2016/1/15 11:08
 * @see
 * @since : 1.0.0
 */
public class TestMessageHandler implements MessageHandler {

    private Logger logger = LoggerFactory.getLogger(TestMessageHandler.class);

    @Override
    public void handleMessage(ConsumerResult result) {
        String str = new String(result.getValue().getMessageByte());
        System.out.println("================="+str);
    }
}
