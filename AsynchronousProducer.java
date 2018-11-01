package org.kafka.practice;


//Asynchronous Sending. Single element and Map elements.

//Limitation max.in.flight.requests.per.connection (5 by default)
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.*;
public class AsynchronousProducer {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		String topic = "MySecTopic";
		
		
		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		/*
		String key = "key1";
		String value = "value1";
		Producer<String, String> producer = new KafkaProducer<String, String>(prop);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
		producer.send(record, new MyCallback());
		producer.close();
		*/
		
		Map<String, String> map = new HashMap<String, String>();
		map.put("key1", "value1");
		map.put("key2", "value2");
		map.put("key3", "value3");
		map.put("key4", "value4");
		map.put("key5", "value5");
		for(Map.Entry<String, String> entry : map.entrySet()) {
			Producer<String, String> producer = new KafkaProducer<String, String>(prop);
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, entry.getKey(), entry.getValue());
			RecordMetadata meta = producer.send(record, new MyProducerCallback()).get();
			System.out.println("Asynchronous Sending Successful. Partition is at " + meta.partition() + " "
					+ "offset is " + meta.offset());
			producer.close();
		}
	}
}

class MyProducerCallback implements Callback{

	@Override
	public void onCompletion(RecordMetadata arg0, Exception e) {
		if(e!=null) {
			System.out.println("Asynchronous sending failed.");
			e.printStackTrace();
		}else System.out.println("Aysnchronous sending successfull.");
	}
	
}
