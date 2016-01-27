package com.mycompany.app;

/**
 * Created by dhiraj on 21/12/15.
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Properties;


public class ProducerTest {

    final static String TOPIC = "test";


    public static void main(String[] argv) {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        //properties.put("serializer.class" , "com.mycompany.app.KafkaSerializer");

        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
        SimpleDateFormat sdf = new SimpleDateFormat();
        StringBuilder sb = new StringBuilder();
        JsonNode jsonObject = null;

            try {
                byte[] Jsondata = Files.readAllBytes(Paths.get("/home/dhiraj/happy/kafka-eg/jsonparse.txt"));
                ObjectMapper objectMapper = new ObjectMapper();
                jsonObject = objectMapper.readValue(Jsondata, JsonNode.class);
                System.out.println(" Json Data parsed using Jackson json parser : "+ jsonObject);
            } catch (IOException e) {
                e.printStackTrace();
            }

         //   System.out.println("Json Object printed in the console \n" + jsonObject);


/*            String jsonData = "{\n" +
                    "  \"id\": 123,\n" +
                    "  \"name\": \"Pankaj\",\n" +
                    "  \"permanent\": true,\n" +
                    "  \"address\": {\n" +
                    "    \"street\": \"Albany Dr\",\n" +
                    "    \"city\": \"San Jose\",\n" +
                    "    \"zipcode\": 95129\n" +
                    "  },\n" +
                    "  \"phoneNumbers\": [\n" +
                    "    123456,\n" +
                    "    987654\n" +
                    "  ],\n" +
                    "  \"role\": \"Manager\",\n" +
                    "  \"cities\": [\n" +
                    "    \"Los Angeles\",\n" +
                    "    \"New York\"\n" +
                    "  ],\n" +
                    "  \"properties\": {\n" +
                    "    \"age\": \"29 years\",\n" +
                    "    \"salary\": \"1000 USD\"\n" +
                    "  }\n" +
                    "}\n";*/


        BufferedWriter writer = null;
        File file = new File("/home/dhiraj/happy/kafka-eg/jsonparse.json");
        try
        {
            writer = new BufferedWriter( new FileWriter( file));
            writer.write( jsonObject.toString());

        }
        catch ( IOException e){}
        finally
        {
            try
            {
                if ( writer != null)writer.close( );
            }
            catch ( IOException e){}
        }



        //KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, jsonObject + sdf.format(new Date()));
        KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, jsonObject.toString());
            producer.send(message);

            producer.close();
        }

    }


