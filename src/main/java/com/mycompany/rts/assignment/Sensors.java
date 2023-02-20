package com.mycompany.rts.assignment;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
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
                    sensorData.addActuatorFeedback(m);
                    System.out.println("Feedback Received - " + m);
                }),
                x -> {
                });

        ScheduledExecutorService flightControl = Executors.newScheduledThreadPool(
                5);

        flightControl.scheduleAtFixedRate(
                sensorData.new Sensor("Altitude"),
                2,
                5,
                TimeUnit.SECONDS);

        flightControl.scheduleAtFixedRate(
                sensorData.new Sensor("Pressure"),
                2,
                5,
                TimeUnit.SECONDS);

        flightControl.scheduleAtFixedRate(
                sensorData.new Sensor("PlaneSpeed"),
                2,
                5,
                TimeUnit.SECONDS);

        flightControl.scheduleAtFixedRate(
                sensorData.new Sensor("Temperature"),
                2,
                5,
                TimeUnit.SECONDS);

        flightControl.scheduleAtFixedRate(
                sensorData.new Sensor("Humidity"),
                2,
                5,
                TimeUnit.SECONDS);

        flightControl.scheduleAtFixedRate(
                sensorData.new Sensor("Rainfall"),
                2,
                5,
                TimeUnit.SECONDS);

        flightControl.scheduleAtFixedRate(
                sensorData.new Sensor("Wind"),
                2,
                5,
                TimeUnit.SECONDS);

        Thread actuatorFeedbackThread = new Thread(sensorData.new ActuatorFeedback());
        actuatorFeedbackThread.start();

    }
}

class SensorData {

    public volatile ArrayList<String> actuatorFeedback = new ArrayList<String>();
    public volatile int altitude = 50;
    public volatile int pressure = 50;
    public volatile int planeSpeed = 50;
    public volatile int temperature = 50;
    public volatile int humidity = 50;
    public volatile int rainfall = 50;

    String exchange = "flightControlExchange";
    String key = "sensorData";

    public synchronized void addActuatorFeedback(String data) {
        actuatorFeedback.add(data);
    }

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

    class Sensor implements Runnable {

        ConnectionFactory cf = new ConnectionFactory();

        String sensorType;

        public Sensor(String sensorType) {
            this.sensorType = sensorType;
        }

        @Override
        public void run() {
            try (Connection con = cf.newConnection()) {
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
                Channel channel = con.createChannel();
                channel.exchangeDeclare(exchange, "direct");
                channel.basicPublish(exchange, key, false, null, msg.getBytes());
                System.out.println("Sensor Data Sent - " + msg);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    class ActuatorFeedback implements Runnable {

        @Override
        public void run() {
            while (true) {
                if (actuatorFeedback.size() > 0) {
                    processActuatorFeedback(actuatorFeedback.get(0));
                    actuatorFeedback.remove(0);
                }
            }
        }

        public void processActuatorFeedback(String feedback) {
            String command = feedback.split(" ")[0]; // Increase or Decrease
            String sensorType = feedback.split(" ")[1]; // Pressure,Temperature,Altitude,PlaneSpeed,Humidity,Rainfall
            System.out.println("Processing Feedback - " + command + " " + sensorType);
            int adjustment = (command.trim() == "Increase") ? 10 : -10;
            switch (sensorType) {
                case "Pressure":
                    System.out.println("Pressure Adjusted");
                    pressure += adjustment;
                    break;
                case "Temperature":
                    System.out.println("Temperature Adjusted");
                    temperature += adjustment;
                    break;
                case "Altitude":
                    System.out.println("Altitude Adjusted");
                    altitude += adjustment;
                    break;
                case "PlaneSpeed":
                    System.out.println("PlaneSpeed Adjusted");
                    planeSpeed += adjustment;
                    break;
                case "Humidity":
                    System.out.println("Humidity Adjusted");
                    humidity += adjustment;
                    break;
                case "Rainfall":
                    System.out.println("Rainfall Adjusted");
                    rainfall += adjustment;
                    break;
            }
        }
    }

}
