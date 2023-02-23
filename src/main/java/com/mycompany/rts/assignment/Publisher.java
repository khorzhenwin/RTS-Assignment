package com.mycompany.rts.assignment;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Publisher {
    protected String publisherExchange;
    protected String publisherKey;
    protected static ConnectionFactory cf = new ConnectionFactory();
    protected static Connection connection;
    protected static Channel channel;
    protected static String QUEUE_NAME;

    public Publisher(String publisherExchange, String publisherKey) {
        this.publisherExchange = publisherExchange;
        this.publisherKey = publisherKey;

        try {
            connection = cf.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(publisherExchange, "direct");
            QUEUE_NAME = channel.queueDeclare().getQueue();
            channel.queueBind(QUEUE_NAME, publisherExchange, publisherKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void publish(String msg) throws IOException, TimeoutException {
        channel.basicPublish(publisherExchange, publisherKey, false, null, msg.getBytes());
        System.out.println("Command Sent - " + msg);
    }
}
