package com.mycompany.app;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by ravi on 24/12/15.
 */
public class SimpleConsumerTest {

    public static void main(String[] args) throws IOException {
        Properties config = new Properties();
        config.put("zookeeper.connect", "localhost:2181");
        config.put("group.id", "default");
        config.put("partition.assignment.strategy", "roundrobin");
        config.put("bootstrap.servers", "localhost:9092");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafka.consumer.ConsumerConfig consumerConfig = new kafka.consumer.ConsumerConfig(config);

        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put("test", 1);
        //  System.out.println("After Topic count map ");
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
     //   System.out.println(" consumerMap :" + consumerMap);

        List<KafkaStream<byte[], byte[]>> streamList = consumerMap.get("test");
    //    KafkaConsumer consumer = new KafkaConsumer(config);
        KafkaStream<byte[], byte[]> stream = streamList.get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        StringBuilder getMessageTopic = new StringBuilder();
        Map<String, ConsumerRecords> records = null;
        while (iterator.hasNext()) {
            System.out.println(new String(iterator.next().message()));
            getMessageTopic = new StringBuilder(iterator.next().message().toString());
        }
        System.out.println("out of while loop");

        System.out.println("entering into try block ");
        //   String content = "This is the content to write into file";

        File file = new File("/home/dhiraj/happy/kafka-eg/kafka_consume1.txt");

        // if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(getMessageTopic.toString());
        bw.close();

        System.out.println("Done");


    }

    public static void processRecords(Map<String, ConsumerRecords<String, String>> records) {
       System.out.println("Entered into process records ");
        List<ConsumerRecord<String, String>> messages = records.get("test").records();
        if(messages != null) {
            for (ConsumerRecord<String, String> next : messages) {
                try {
                    System.out.println(next.value());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println("No messages");
        }
    }
}
