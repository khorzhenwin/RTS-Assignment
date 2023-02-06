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
      ArrayList<String> sensorData = new ArrayList<String>();

      Connection connection = cf.newConnection();
      Channel channel = connection.createChannel();

      channel.exchangeDeclare(exchange, "direct");
      String QUEUE_NAME = channel.queueDeclare().getQueue();
      channel.queueBind(QUEUE_NAME, exchange, key);

      channel.basicConsume(
            QUEUE_NAME,
            ((x, msg) -> {
               String m = new String(msg.getBody(), "utf-8");
               System.out.println(m);
               // add message to arrayList
               sensorData.add(m);
            }),
            x -> {
            });

      // process sensor data through the arrayList
      while (sensorData.size() > 0) {
         String data = sensorData.get(0);
         String sensorType = data.split(" ")[0].trim();
         String reading = data.split(":")[1].trim();

         if (sensorType != "Wind"
               && (Integer.valueOf(reading) < 30 || Integer.valueOf(reading) > 70)) {
            int sensorValue = Integer.valueOf(reading);
            System.out.println(getBalancingMessage(sensorType, sensorValue));
            ArrayList<String> commands = getCommands(
                  sensorType,
                  sensorValue);
            for (int i = 0; i < commands.size(); i++) {
               System.out.println(commands.get(i));
               Thread thread = new Thread(new CommandLogic(commands.get(i)));
               thread.start();
            }
         } else if (sensorType == "Wind") {
            // String direction = reading.split("-")[0].trim();
            // String speed = reading.split("-")[1].trim();
            // if (direction == "NONE") {
            // new CommandLogic("TurnLeft").run();
            // } else if (direction == "NORTH") {
            // new CommandLogic("TurnRight").run();
            // } else if (direction == "SOUTH") {
            // new CommandLogic("TurnLeft").run();
            // } else if (direction == "EAST") {
            // new CommandLogic("TurnLeft").run();
            // } else if (direction == "WEST") {
            // new CommandLogic("TurnRight").run();
            // }
         }

         sensorData.remove(0);
      }
   }

   static ArrayList<String> getCommands(String sensorType, int sensorValue) {
      ArrayList<String> commands = new ArrayList<String>();
      switch (sensorType) {
         case "Altitude":
            if (sensorValue < 30) {
               commands.add("Increase Engine Speed");
               commands.add("Raise WingFlaps");
               commands.add("Raise TailFlaps");
            } else {
               commands.add("Decrease Engine Speed");
               commands.add("Lower WingFlaps");
               commands.add("Lower TailFlaps");
            }
            break;
         case "Pressure":
            if (sensorValue < 30) {
               commands.add("Increase Engine Speed");
               commands.add("Raise WingFlaps");
               commands.add("Raise TailFlaps");
            } else {
               commands.add("Decrease Engine Speed");
               commands.add("Lower WingFlaps");
               commands.add("Lower TailFlaps");
            }
            break;
         case "PlaneSpeed":
            break;
         case "Temperature":
            break;
         case "Humidity":
            break;
         case "Rainfall":
            break;
      }
      return commands;
   }

   static String getBalancingMessage(String sensorType, int sensorValue) {
      if (sensorValue < 30) {
         return "Low " + sensorType + " detected. Balancing...";
      } else if (sensorValue > 70) {
         return "High " + sensorType + " detected. Balancing...";
      }
      return "";
   }
}

class CommandLogic implements Runnable {

   String exchange = "actuatorExchange";
   String key = "commandData";
   ConnectionFactory cf = new ConnectionFactory();

   String command;

   public CommandLogic(String command) {
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
