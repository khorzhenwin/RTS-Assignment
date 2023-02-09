package com.mycompany.rts.assignment;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

public class FlightControl {

   public static void main(String[] args) throws IOException, TimeoutException {
      String exchange = "flightControlExchange";
      String key = "sensorData";

      ConnectionFactory cf = new ConnectionFactory();

      Connection connection = cf.newConnection();
      Channel channel = connection.createChannel();

      channel.exchangeDeclare(exchange, "direct");
      String QUEUE_NAME = channel.queueDeclare().getQueue();
      channel.queueBind(QUEUE_NAME, exchange, key);

      Command command = new Command();

      channel.basicConsume(
            QUEUE_NAME,
            ((x, msg) -> {
               String m = new String(msg.getBody(), "utf-8");
               command.addSensorData(m);
               System.out.println("Sensor Data Reading - " + m);
            }),
            x -> {
            });

      Thread thread = new Thread(command.new CommandLogic());
      thread.start();
   }

}

class Command {
   public volatile ArrayList<String> sensorData = new ArrayList<String>();

   public synchronized void addSensorData(String data) {
      sensorData.add(data);
   }

   public synchronized int getSensorDataSize() {
      return sensorData.size();
   }

   class CommandLogic implements Runnable {

      @Override
      public void run() {
         while (true) {
            if (getSensorDataSize() > 0) {
               relayCommands(sensorData.get(0));
               sensorData.remove(0);
            }
         }
      }

      void relayCommands(String data) {
         String sensorType = data.split(" ")[0].trim();
         String reading = data.split(":")[1].trim();
         if (!(sensorType.equals("Wind")) && (Integer.valueOf(reading) < 30 || Integer.valueOf(reading) > 70)) {
            int sensorValue = Integer.valueOf(reading);
            System.out.println(getBalancingMessage(sensorType, sensorValue));
            ArrayList<String> commands = getCommands(
                  sensorType,
                  sensorValue);
            for (int i = 0; i < commands.size(); i++) {
               Thread thread = new Thread(new CommandExchange(commands.get(i)));
               thread.start();
            }
         } else if (sensorType.equals("Wind")) {
            // String direction = reading.split("|")[0].trim();
            // String speed = reading.split("|")[1].trim();
         } else {
            System.out.println("Normal reading for " + sensorType + " sensor. No action required.");
         }
      }

      ArrayList<String> getCommands(String sensorType, int sensorValue) {
         ArrayList<String> commands = new ArrayList<String>();
         switch (sensorType) {
            case "Altitude":
               if (sensorValue < 30) {
                  commands.add("Increase Altitude");
                  commands.add("Raise WingFlaps");
                  commands.add("Raise TailFlaps");
               } else {
                  commands.add("Decrease Altitude");
                  commands.add("Lower WingFlaps");
                  commands.add("Lower TailFlaps");
               }
               break;
            case "Pressure":
               if (sensorValue < 30 && sensorValue > 10) {
                  commands.add("Increase Pressure - Repressurizing Cabin");
               } else if (sensorValue > 70) {
                  commands.add("Decrease Pressure - Venting Cabin");
               } else if (sensorValue < 10 || sensorValue > 90) {
                  commands.add("Emergency : Deployment of Oxygen Masks");
               }
               break;
            case "PlaneSpeed":
               if (sensorValue < 30) {
                  commands.add("Increase PlaneSpeed");
                  commands.add("Increase EngineThrust");
               } else {
                  commands.add("Decrease PlaneSpeed");
                  commands.add("Decrease EngineThrust");
               }
               break;
            case "Temperature":
               if (sensorValue < 30) {
                  commands.add("Increase Temperature");
               } else {
                  commands.add("Decrease Temperature");
               }
               break;
            case "Humidity":
               if (sensorValue < 30) {
                  commands.add("Increase Humidity");
               } else {
                  commands.add("Decrease Humidity");
               }
               break;
            case "Rainfall":
               if (sensorValue > 80) {
                  commands.add("Emergency : Heavy Rainfall Detected");
                  commands.add("Emergency : Pinging Nearest Airports");
                  commands.add("Emergency : Deploy Landing Gear");
               }
               break;
         }
         return commands;
      }

      String getBalancingMessage(String sensorType, int sensorValue) {
         if (sensorValue < 30) {
            return "Low " + sensorType + " detected. Balancing...";
         } else if (sensorValue > 70) {
            return "High " + sensorType + " detected. Balancing...";

         }
         return "";
      }
   }
}

class CommandExchange implements Runnable {

   String exchange = "actuatorExchange";
   String key = "commandData";
   ConnectionFactory cf = new ConnectionFactory();

   String command;

   public CommandExchange(String command) {
      this.command = command;
   }

   @Override
   public void run() {
      try (Connection con = cf.newConnection()) {
         Channel channel = con.createChannel();
         channel.exchangeDeclare(exchange, "direct");
         channel.basicPublish(exchange, key, false, null, command.getBytes());
         System.out.println("Command Sent - " + command);
      } catch (Exception e) {
      }
   }
}
