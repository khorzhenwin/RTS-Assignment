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
                3);
        flightControl.scheduleAtFixedRate(
                sensorData.new AltitudeSensor(),
                2,
                5,
                TimeUnit.SECONDS);
        flightControl.scheduleAtFixedRate(
                sensorData.new PressureSensor(),
                2,
                5,
                TimeUnit.SECONDS);
        flightControl.scheduleAtFixedRate(
                sensorData.new PlaneSpeedSensor(),
                2,
                5,
                TimeUnit.SECONDS);
        flightControl.scheduleAtFixedRate(
                sensorData.new TemperatureSensor(),
                2,
                5,
                TimeUnit.SECONDS);
        flightControl.scheduleAtFixedRate(
                sensorData.new WindSensor(),
                2,
                5,
                TimeUnit.SECONDS);
        flightControl.scheduleAtFixedRate(
                sensorData.new HumiditySensor(),
                2,
                5,
                TimeUnit.SECONDS);
        flightControl.scheduleAtFixedRate(
                sensorData.new RainfallSensor(),
                2,
                5,
                TimeUnit.SECONDS);
    }
}

class SensorData {

    String exchange = "flightControlExchange";
    String key = "sensorData";
    ConnectionFactory cf = new ConnectionFactory();

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

    class AltitudeSensor implements Runnable {

        int initialReading = 50;

        @Override
        public void run() {
            try (Connection con = cf.newConnection()) {
                initialReading += getRandomValue();
                String msg = "Altitude Reading : " + initialReading;
                Channel channel = con.createChannel();
                channel.exchangeDeclare(exchange, "direct");
                channel.basicPublish(exchange, key, false, null, msg.getBytes());
                System.out.println("Sensor Data Sent - " + msg);
            } catch (Exception e) {
            }
        }
    }

    class PressureSensor implements Runnable {

        int initialReading = 50;

        @Override
        public void run() {
            try (Connection con = cf.newConnection()) {
                initialReading += getRandomValue();
                String msg = "Pressure Reading : " + initialReading;
                Channel channel = con.createChannel();
                channel.exchangeDeclare(exchange, "direct");
                channel.basicPublish(exchange, key, false, null, msg.getBytes());
                System.out.println("Sensor Data Sent - " + msg);
            } catch (Exception e) {
            }
        }
    }

    class PlaneSpeedSensor implements Runnable {

        int initialReading = 50;

        @Override
        public void run() {
            try (Connection con = cf.newConnection()) {
                initialReading += getRandomValue();
                String msg = "PlaneSpeed Reading : " + initialReading;
                Channel channel = con.createChannel();
                channel.exchangeDeclare(exchange, "direct");
                channel.basicPublish(exchange, key, false, null, msg.getBytes());
                System.out.println("Sensor Data Sent - " + msg);
            } catch (Exception e) {
            }
        }
    }

    class TemperatureSensor implements Runnable {

        int initialReading = 50;

        @Override
        public void run() {
            try (Connection con = cf.newConnection()) {
                initialReading += getRandomValue();
                String msg = "Temperature Reading : " + initialReading;
                Channel channel = con.createChannel();
                channel.exchangeDeclare(exchange, "direct");
                channel.basicPublish(exchange, key, false, null, msg.getBytes());
                System.out.println("Sensor Data Sent - " + msg);
            } catch (Exception e) {
            }
        }
    }

    class WindSensor implements Runnable {

        String direction = getRandomDirection();
        String msg = (direction == "NONE")
                ? "Wind Reading : " + direction + "-0"
                : "Wind Reading : " + direction + "-" + getRandomValue();

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

    class HumiditySensor implements Runnable {

        int initialReading = 50;

        @Override
        public void run() {
            try (Connection con = cf.newConnection()) {
                initialReading += getRandomValue();
                if (initialReading < 0)
                    initialReading = 0;
                String msg = "Humidity Reading : " + initialReading;
                Channel channel = con.createChannel();
                channel.exchangeDeclare(exchange, "direct");
                channel.basicPublish(exchange, key, false, null, msg.getBytes());
                System.out.println("Sensor Data Sent - " + msg);
            } catch (Exception e) {
            }
        }
    }

    class RainfallSensor implements Runnable {

        int initialReading = 50;

        @Override
        public void run() {
            try (Connection con = cf.newConnection()) {
                initialReading += getRandomValue();
                if (initialReading < 0)
                    initialReading = 0;
                String msg = "Rainfall Reading : " + initialReading;
                Channel channel = con.createChannel();
                channel.exchangeDeclare(exchange, "direct");
                channel.basicPublish(exchange, key, false, null, msg.getBytes());
                System.out.println("Sensor Data Sent - " + msg);
            } catch (Exception e) {
            }
        }
    }
}
