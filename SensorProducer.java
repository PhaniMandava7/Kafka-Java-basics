package org.kafka.practice;

import java.util.*;
import org.apache.kafka.clients.producer.*;

/*
 * Default Partitioner :
 * 	1. If the partition number and Key are not specified, Default Partitioner would assign choose a default partition by Round-robin.
 * 	2. If the partition number is not specified but a Key is specified, Default Partitioner would calculate the partition is calculated based on Hash key.
 * 	3. If the partition number is specified, record will use it.
 * 
 *  Problem with Default Partitioner:
 *  	1. Hashing guarantees that a key will always give same number., but it doesn't ensure 2 different keys will never give you same number(records with 2 different keys may give same partitioner number).
 *  	2. Partition number = (Hash value for the key) % (Total number of partitions for the topic)... The number of partitions may vary over-time and the Default partitioner may return different values for same key.
 *  
 *   So it's not reliable to use Default Partitioner.
 *  
 */

public class SensorProducer {
	public static void main(String[] args) {
		String topic = "MyPartitionerTopic";
		
		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//partitioner class is the fully qualified name of Custom Partitioner class.
		prop.put("partitioner.class", "org.kafka.practice.SensorPartitioner");
		//speed.sensor.name is the config that is used by Custom Partitioner.
		prop.put("sensor.name", "TSS");
		
		Producer<String, String> producer = new KafkaProducer<String, String>(prop);
		for(int i =0 ; i<10; i++) 
			producer.send(new ProducerRecord<String, String>(topic, "SSR"+i , "500"+i));
		for(int i =0; i<10; i++) 
			producer.send(new ProducerRecord<String, String>(topic, "TSS", "500"+i));

		producer.close();
	}
}
