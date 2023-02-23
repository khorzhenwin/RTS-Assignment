package com.mycompany.rts.assignment;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Sensors {

    public static void main(String[] args) throws IOException, TimeoutException {
        String exchange = "sensorExchange";
        String key = "actuatorData";
        SensorData sensorData = new SensorData();
        ConnectionFactory cf = new ConnectionFactory();

        Connection connection = cf.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(exchange, "direct");
        String QUEUE_NAME = channel.queueDeclare().getQueue();
        channel.queueBind(QUEUE_NAME, exchange, key);

        channel.basicConsume(
                QUEUE_NAME,
                ((x, msg) -> {
                    String m = new String(msg.getBody(), "utf-8");
                    System.out.println("Feedback Received - " + m);
                    sensorData.processActuatorFeedback(m);
                }),
                x -> {
                });

        ScheduledExecutorService flightControl = Executors.newScheduledThreadPool(7);
        String[] sensorTypes = { "Altitude", "Pressure", "PlaneSpeed", "Temperature", "Humidity", "Rainfall", "Wind" };
        for (String sensorType : sensorTypes) {
            flightControl.scheduleAtFixedRate(
                    sensorData.new PublishSensorData(sensorType),
                    2,
                    5,
                    TimeUnit.SECONDS);
        }

    }
}

class SensorData {
    public volatile int altitude = 50;
    public volatile int pressure = 50;
    public volatile int planeSpeed = 50;
    public volatile int temperature = 50;
    public volatile int humidity = 50;
    public volatile int rainfall = 50;

    public synchronized static int getRandomValue() {
        // random value from -10 to 10
        Random random = new Random();
        int select = random.nextInt(21);
        return select - 10;
    }

    public synchronized String getRandomDirection() {
        String[] readings = { "NORTH", "SOUTH", "EAST", "WEST", "NONE" };
        Random random = new Random();
        int select = random.nextInt(readings.length);
        return readings[select];
    }

    public void processActuatorFeedback(String feedback) {
        String command = feedback.split(" ")[0]; // Increase or Decrease
        String sensorType = feedback.split(" ")[1]; // Pressure,Temperature,Altitude,PlaneSpeed,Humidity,Rainfall
        int adjustment = (command.trim() == "Increase") ? 10 : -10;
        switch (sensorType) {
            case "Pressure":
                pressure += adjustment;
                System.out.println("Adjusted " + sensorType + " Reading : " + pressure);
                break;
            case "Temperature":
                temperature += adjustment;
                System.out.println("Adjusted " + sensorType + " Reading : " + temperature);
                break;
            case "Altitude":
                altitude += adjustment;
                System.out.println("Adjusted " + sensorType + " Reading : " + altitude);
                break;
            case "PlaneSpeed":
                planeSpeed += adjustment;
                System.out.println("Adjusted " + sensorType + " Reading : " + planeSpeed);
                break;
            case "Humidity":
                humidity += adjustment;
                System.out.println("Adjusted " + sensorType + " Reading : " + humidity);
                break;
            case "Rainfall":
                rainfall += adjustment;
                System.out.println("Adjusted " + sensorType + " Reading : " + rainfall);
                break;
        }
    }

    class PublishSensorData extends Publisher implements Runnable {
        String sensorType;

        public PublishSensorData(String sensorType) {
            super("flightControlExchange", "sensorData");
            this.sensorType = sensorType;
        }

        @Override
        public void run() {
            try {
                String msg = getPublishMessage(sensorType);
                publish(msg);
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        public String getPublishMessage(String sensorType) {
            String msg = "";
            switch (sensorType) {
                case "Altitude":
                    altitude += getRandomValue();
                    msg = sensorType + " Reading : " + altitude;
                    break;
                case "Pressure":
                    pressure += getRandomValue();
                    msg = sensorType + " Reading : " + pressure;
                    break;
                case "PlaneSpeed":
                    planeSpeed += getRandomValue();
                    msg = sensorType + " Reading : " + planeSpeed;
                    break;
                case "Temperature":
                    temperature += getRandomValue();
                    msg = sensorType + " Reading : " + temperature;
                    break;
                case "Humidity":
                    humidity += getRandomValue();
                    msg = sensorType + " Reading : " + humidity;
                    break;
                case "Rainfall":
                    rainfall += getRandomValue();
                    msg = sensorType + " Reading : " + rainfall;
                    break;
                case "Wind":
                    String direction = getRandomDirection();
                    msg = (direction == "NONE")
                            ? "Wind Reading : " + direction + "@0"
                            : "Wind Reading : " + direction + "@" + getRandomValue();
            }
            return msg;
        }

    }

}
