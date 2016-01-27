package com.mycompany.app;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Created by ravi on 21/12/15.
 */
public class ConsumerTest extends Thread {


    final static String clientId = "Demo1";
    final static String TOPIC = "test";
    ConsumerConnector consumerConnector;


    public static void main(String[] argv) throws UnsupportedEncodingException {
        ConsumerTest consumerTest = new ConsumerTest();
        consumerTest.start();
    }

    public ConsumerTest(){
        String topic = "test";
        Properties properties = new Properties();
        properties.put("zookeeper.connect","localhost:2181");
        properties.put("group.id","test-group");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        String value = null;
                // Create ConsumerConnector with createJavaConsumerConnector
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        // Create a map of topics we are interested in with the number of
        // streams (usually threads) to service the topic
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);

        // Get the list of streams and configure it to use Strings
        Map<String, List<KafkaStream<String, String>>> consumerMap =
                consumerConnector.createMessageStreams(topicCountMap, new StringDecoder(null), new StringDecoder(null));

        // Get the stream for the topic we want to consume
        KafkaStream<String, String> stream = consumerMap.get(topic).get(0);

        // Iterate through all of the messages in the stream
        ConsumerIterator<String, String> it = stream.iterator();


        JsonNode jsonObject = null;
        while (it.hasNext()) {
            MessageAndMetadata<String, String> messageAndMetadata = it.next();

            String key = messageAndMetadata.key();
          //  System.out.println("Key : "+ key);
           value = messageAndMetadata.message();

          //  System.out.println("Key is \"" + key + "\" value is \"" + value + "\"");

            ObjectMapper objectMapper = new ObjectMapper();
            try {
                jsonObject = objectMapper.readValue(value.toString(), JsonNode.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println(" Json Data parsed using Jackson json parser : "+ jsonObject);

            System.out.println("Value fetched from the kafka server : "+ value);
        }


        consumerConnector.shutdown();

    }


    public void run() {
       System.out.print("Inside run method ");
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream =  consumerMap.get(TOPIC).get(0);

      System.out.println(" stream : "+ stream);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while(it.hasNext())
            System.out.println(new String(it.next().message()));

    }

   /* private static void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
        for(MessageAndOffset messageAndOffset: messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(new String(bytes, "UTF-8"));

        }
    }*/

}


