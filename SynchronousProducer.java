package org.kafka.practice;

import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.*;

//Synchronous Sending. Single element and Map elements.
public class SynchronousProducer {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		String topic = "MySecTopic";
		
		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092");
		prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = new KafkaProducer<String, String>(prop);
		/* Sending a single element.
		String key = "key1";
		String value = "value1";
		
		ProducerRecord <String, String>record = new ProducerRecord<String, String>(topic,key,value);
		try {
			RecordMetadata meta = producer.send(record).get();
			System.out.println("Sending successful. Partition at " + meta.partition() + " offset is " + meta.offset());
		} catch(Exception e) {
			System.out.println(e.getStackTrace());
		}finally {
			producer.close();
		}
		*/
		
		Map<String, String> map = new HashMap<String,String>();
		map.put("key3", "value3");
		map.put("key4", "value4");
		map.put("key5", "value5");
		map.put("key2", "value2");
		for(Map.Entry<String, String> entry : map.entrySet()) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, entry.getKey(), entry.getValue());
			try {
				RecordMetadata meta = producer.send(record).get();
				System.out.println("Sending succesfful. Partition at " + meta.partition() + " offset is " + meta.offset());
			}catch(Exception e) {
				e.printStackTrace();
			} 
		}
		producer.close();
	}
}
