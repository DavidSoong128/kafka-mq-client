package com.horizon.test.consumer;

import com.horizon.mqclient.api.AutoCommitMessageHandler;
import com.horizon.mqclient.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : David.Song/Java Engineer
 * @date : 2016/1/15 11:08
 * @see
 * @since : 1.0.0
 */
public class TestMessageHandler extends AutoCommitMessageHandler {

    private Logger logger = LoggerFactory.getLogger(TestMessageHandler.class);

    @Override
    public void handleMessage(Message message) {
        String str = new String(message.getMessageByte());
        System.out.println("================="+str);
    }
}
