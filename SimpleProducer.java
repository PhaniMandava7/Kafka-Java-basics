package org.kafka.practice;

import java.util.*;
import org.apache.kafka.clients.producer.*;

//Fire and Forget Approach.
//Simple Producer Program to send a map of Key values to Kafka broker.
public class SimpleProducer {
	public static void main(String[] args) {
		System.out.println("Producer Started");
		Map<String, String> map = new HashMap<>();
		map.put("key1", "value1");
		map.put("key2", "value2");
		map.put("key3", "value2");
		map.put("key4", "value4");
		map.put("key5", "value5");
		String topic = "MySecTopic";
		
		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = new KafkaProducer<String, String>(prop);
		for (Map.Entry<String, String> entry : map.entrySet()) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,entry.getKey(),entry.getValue());
			producer.send(record);
		}
		producer.close();
	}
}
