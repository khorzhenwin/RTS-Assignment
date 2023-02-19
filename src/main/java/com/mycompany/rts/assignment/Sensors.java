package com.mycompany.rts.assignment;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Sensors {

    public static void main(String[] args) {
        SensorData sensorData = new SensorData();
        ScheduledExecutorService flightControl = Executors.newScheduledThreadPool(
                5);

        flightControl.scheduleAtFixedRate(
                sensorData.new Sensor(50, "Altitude"),
                2,
                5,
                TimeUnit.SECONDS);

        flightControl.scheduleAtFixedRate(
                sensorData.new Sensor(50, "Pressure"),
                2,
                5,
                TimeUnit.SECONDS);

        flightControl.scheduleAtFixedRate(
                sensorData.new Sensor(50, "PlaneSpeed"),
                2,
                5,
                TimeUnit.SECONDS);

        flightControl.scheduleAtFixedRate(
                sensorData.new Sensor(50, "Temperature"),
                2,
                5,
                TimeUnit.SECONDS);

        flightControl.scheduleAtFixedRate(
                sensorData.new Sensor(50, "Humidity"),
                2,
                5,
                TimeUnit.SECONDS);

        flightControl.scheduleAtFixedRate(
                sensorData.new Sensor(50, "Rainfall"),
                2,
                5,
                TimeUnit.SECONDS);

        flightControl.scheduleAtFixedRate(
                sensorData.new WindSensor(),
                2,
                5,
                TimeUnit.SECONDS);

    }
}

class SensorData {

    String exchange = "flightControlExchange";
    String key = "sensorData";

    class Sensor implements Runnable {
        ConnectionFactory cf = new ConnectionFactory();

        int initialReading;
        String sensorType;

        public Sensor(int initialReading, String sensorType) {
            this.initialReading = initialReading;
            this.sensorType = sensorType;
        }

        @Override
        public void run() {
            try (Connection con = cf.newConnection()) {
                initialReading += getRandomValue();
                initialReading = (initialReading < 0) ? 0 : initialReading;
                String msg = sensorType + " Reading : " + initialReading;
                Channel channel = con.createChannel();
                channel.exchangeDeclare(exchange, "direct");
                channel.basicPublish(exchange, key, false, null, msg.getBytes());
                System.out.println("Sensor Data Sent - " + msg);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    class WindSensor implements Runnable {
        ConnectionFactory cf = new ConnectionFactory();

        String direction = getRandomDirection();
        String msg = (direction == "NONE")
                ? "Wind Reading : " + direction + "@0"
                : "Wind Reading : " + direction + "@" + getRandomValue();

        @Override
        public void run() {
            try (Connection con = cf.newConnection()) {
                Channel channel = con.createChannel();
                channel.exchangeDeclare(exchange, "direct");
                channel.basicPublish(exchange, key, false, null, msg.getBytes());
                System.out.println("Sensor Data Sent - " + msg);
            } catch (Exception e) {
            }
        }
    }

    public static int getRandomValue() {
        // random value from -10 to 10
        Random random = new Random();
        int select = random.nextInt(21);
        return select - 10;
    }

    public String getRandomDirection() {
        String[] readings = { "NORTH", "SOUTH", "EAST", "WEST", "NONE" };
        Random random = new Random();
        int select = random.nextInt(readings.length);
        return readings[select];
    }
}
