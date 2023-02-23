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

public class FlightControl {

   public static void main(String[] args) throws IOException, TimeoutException {
      String exchange = "flightControlExchange";
      String key = "sensorData";
      CommandData command = new CommandData();

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
               command.addSensorData(m);
               System.out.println("Sensor Data Reading - " + m);
            }),
            x -> {
            });

      ScheduledExecutorService flightControl = Executors.newScheduledThreadPool(2);
      flightControl.scheduleAtFixedRate(
            command.new CommandProcessor(),
            2,
            5,
            TimeUnit.SECONDS);
   }

}

class CommandData {
   public volatile ArrayList<String> sensorData = new ArrayList<String>();

   public synchronized void addSensorData(String data) {
      sensorData.add(data);
   }

   class CommandProcessor extends Publisher implements Runnable {
      public CommandProcessor() throws IOException, TimeoutException {
         super("actuatorExchange", "commandData");
      }

      @Override
      public void run() {
         while (true) {
            try {
               if (sensorData.size() > 0) {
                  publishCommand(sensorData.get(0));
                  sensorData.remove(0);
               }
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      }

      void publishCommand(String data) throws IOException, TimeoutException {
         ArrayList<String> commands = new ArrayList<String>();
         String sensorType = data.split(" ")[0].trim();
         String reading = data.split(":")[1].trim();
         boolean isWindSensor = sensorType.equals("Wind");

         int sensorValue = (!isWindSensor) ? Integer.valueOf(reading) : Integer.valueOf(reading.split("@")[1].trim());
         commands = getCommands(sensorType, sensorValue);

         for (int i = 0; i < commands.size(); i++) {
            Thread thread = new Thread(new PublishCommandData(commands.get(i)));
            thread.start();
         }
      }

      ArrayList<String> getCommands(String sensorType, int sensorValue) {
         ArrayList<String> commands = new ArrayList<String>();
         if (sensorValue > 30 && sensorValue < 70) {
            return commands;
         }
         System.out.println(getBalancingMessage(sensorType, sensorValue));

         String instruction = (sensorValue < 30 && sensorValue > 10)
               ? "Increase"
               : (sensorValue > 70 && sensorValue < 90)
                     ? "Decrease"
                     : "Emergency";

         switch (sensorType) {
            case "Altitude":
               String flapInstruction = (instruction.equals("Increase")) ? "Raise" : "Lower";
               commands.add(instruction + " Altitude");
               commands.add(flapInstruction + " WingFlaps");
               commands.add(flapInstruction + " TailFlaps");
               break;
            case "Pressure":
               String command = (instruction.equals("Emergency")) ? "Emergency : Deploy Oxygen Masks"
                     : instruction + " Pressure";
               commands.add(command);
               break;
            case "PlaneSpeed":
               commands.add(instruction + " PlaneSpeed");
               commands.add(instruction + " EngineThrust");
               break;
            case "Temperature":
               commands.add(instruction + " Temperature");
               break;
            case "Humidity":
               commands.add(instruction + " Humidity");
               break;
            case "Rainfall":
               if (sensorValue > 80) {
                  commands.add("Emergency : Heavy Rainfall Detected");
                  commands.add("Emergency : Pinging Nearest Airports");
                  commands.add("Emergency : Deploy Landing Gear");
               }
               break;
            case "Wind":
               if (sensorValue < -5) {
                  // going too fast, raise flags
                  commands.add("Backward Wind Detected");
                  commands.add("Raise WingFlaps");
                  commands.add("Raise TailFlaps");
               } else if (sensorValue > 5) {
                  // going too slow, lower flags
                  commands.add("Forward Wind Detected");
                  commands.add("Lower WingFlaps");
                  commands.add("Lower TailFlaps");
               }
               break;
         }
         return commands;
      }

      String getBalancingMessage(String sensorType, int sensorValue) {
         String balanceMessage;
         boolean isWindSensor = sensorType.equals("Wind");
         boolean isRainfallSensor = sensorType.equals("Rainfall");
         if (isWindSensor) {
            balanceMessage = (sensorValue == 0) ? "" : "Wind Speed Detected. Balancing ...";
         } else if (isRainfallSensor) {
            balanceMessage = (sensorValue < 80) ? "" : "Heavy Rainfall Detected";
         } else {
            String lowOrHigh = (sensorValue < 30) ? "Low " : "High ";
            balanceMessage = lowOrHigh + sensorType + " detected. Balancing...";
         }
         return balanceMessage;
      }
   }
}

class PublishCommandData extends Publisher implements Runnable {
   String command;

   public PublishCommandData(String command) throws IOException,
         TimeoutException {
      super("actuatorExchange", "commandData");
      this.command = command;
   }

   @Override
   public void run() {
      try {
         publish(command);
      } catch (Exception e) {
      }
   }
}
