package com.mycompany.rts.assignment;

import java.util.ArrayList;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class ConsumerClass {
    protected String consumerExchange;
    protected String consumerKey;
    protected static ConnectionFactory cf = new ConnectionFactory();
    protected static Connection connection;
    protected static Channel channel;
    protected static String QUEUE_NAME;

    protected volatile ArrayList<String> dataList = new ArrayList<String>();

    public ConsumerClass(String consumerExchange, String consumerKey) {
        this.consumerExchange = consumerExchange;
        this.consumerKey = consumerKey;

        try (Connection consumerConnection = cf.newConnection()) {
            connection = cf.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(consumerExchange, "direct");
            QUEUE_NAME = channel.queueDeclare().getQueue();
            channel.queueBind(QUEUE_NAME, consumerExchange, consumerKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ArrayList<String> getDataList() {
        return dataList;
    }

    public synchronized void addData(String data) {
        dataList.add(data);
    }

    public synchronized void deleteData(int index) {
        dataList.remove(index);
    }

    public synchronized String getAndRemoveFirstItem() {
        String data = dataList.get(0);
        dataList.remove(0);
        return data;
    }

    public synchronized int getDataListSize() {
        return dataList.size();
    }

    public synchronized void consume() {
        try {
            channel.basicConsume(
                    Publisher.QUEUE_NAME,
                    ((x, msg) -> {
                        String m = new String(msg.getBody(), "utf-8");
                        dataList.add(m);
                        System.out.println("Message Received - " + m);
                    }),
                    x -> {
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}