package com.mycompany.rts.assignment;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Actuators {

    public static void main(String[] args) throws IOException, TimeoutException {
        ActuatorData actuatorData = new ActuatorData();
        ScheduledExecutorService flightControl = Executors.newScheduledThreadPool(
                2);
        flightControl.scheduleAtFixedRate(
                actuatorData.new Actuator(),
                2,
                5,
                TimeUnit.SECONDS);
        flightControl.scheduleAtFixedRate(
                actuatorData.new Processor(),
                2,
                5,
                TimeUnit.SECONDS);

        String exchange = "actuatorExchange";
        String key = "commandData";

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
                    actuatorData.addCommand(m);
                    System.out.println("Command Received - " + m);
                }),
                x -> {
                });
    }

}

class ActuatorData {
    public volatile ArrayList<String> commandList = new ArrayList<String>();
    public volatile ArrayList<String> feedbackList = new ArrayList<String>();

    public volatile int tailFlaps = 0;
    public volatile int wingFlaps = 0;
    public volatile int engineThrust = 50;
    public volatile int cabinPressure = 50;
    public volatile int humidity = 50;
    public volatile int temperature = 50;
    public volatile boolean isOxygenMask = false;
    public volatile boolean isLandingGear = false;

    public synchronized void addCommand(String data) {
        commandList.add(data);
    }

    class Actuator implements Runnable {

        @Override
        public void run() {
            System.out.println("Tail Flaps: " + tailFlaps);
            System.out.println("Wing Flaps: " + wingFlaps);
            System.out.println("Engine: " + engineThrust);
            System.out.println("Pressurizer: " + cabinPressure);
            System.out.println("Dehumidifier: " + humidity);
            System.out.println("Heater: " + temperature);
            System.out.println("Oxygen Mask: " + isOxygenMask);
            System.out.println("Landing Gear: " + isLandingGear);
            System.out.println();
        }
    }

    class Processor implements Runnable {
        @Override
        public void run() {
            if (commandList.size() > 0) {
                processCommands(commandList.get(0));
                commandList.remove(0);
                for (int i = 0; i < feedbackList.size(); i++) {
                    Thread feedbackThread = new Thread(new Feedback(feedbackList.get(i)));
                    feedbackThread.start();
                }
            }
        }

        void processCommands(String command) {
            switch (command) {
                case "Emergency : Deploy Landing Gear":
                    isLandingGear = true;
                    break;
                case "Emergency : Oxygen Mask":
                    isOxygenMask = true;
                    break;
                case "Raise WingFlaps":
                    wingFlaps += 5;
                    break;
                case "Lower WingFlaps":
                    wingFlaps -= 5;
                    break;
                case "Raise TailFlaps":
                    tailFlaps += 5;
                    break;
                case "Lower TailFlaps":
                    tailFlaps -= 5;
                    break;
                case "Increase EngineThrust":
                    engineThrust += 5;
                    break;
                case "Decrease EngineThrust":
                    engineThrust -= 5;
                    break;
                case "Increase Pressure - Repressurizing Cabin":
                    cabinPressure += 5;
                    feedbackList.add("Increase Pressure");
                    break;
                case "Decrease Pressure - Venting Cabin":
                    cabinPressure -= 5;
                    feedbackList.add("Decrease Pressure");
                    break;
                case "Increase Humidity":
                    humidity += 5;
                    feedbackList.add("Increase Humidity");
                    break;
                case "Decrease Humidity":
                    humidity -= 5;
                    feedbackList.add("Decrease Humidity");
                    break;
                case "Increase Temperature":
                    temperature += 5;
                    feedbackList.add("Increase Temperature");
                    break;
                case "Decrease Temperature":
                    temperature -= 5;
                    feedbackList.add("Decrease Temperature");
                    break;
                case "Increase Altitude":
                    feedbackList.add("Increase Altitude");
                    break;
                case "Decrease Altitude":
                    feedbackList.add("Decrease Altitude");
                    break;
                case "Increase PlaneSpeed":
                    feedbackList.add("Increase PlaneSpeed");
                    break;
                case "Decrease PlaneSpeed":
                    feedbackList.add("Decrease PlaneSpeed");
                    break;
            }

        }
    }

    class Feedback implements Runnable {
        String exchange = "sensorExchange";
        String key = "actuatorData";
        ConnectionFactory cf = new ConnectionFactory();
        String feedback;

        public Feedback(String feedback) {
            this.feedback = feedback;
        }

        @Override
        public void run() {
            try (Connection con = cf.newConnection()) {
                Channel channel = con.createChannel();
                channel.exchangeDeclare(exchange, "direct");
                channel.basicPublish(exchange, key, false, null, feedback.getBytes());
            } catch (Exception e) {
            }
        }
    }

}
