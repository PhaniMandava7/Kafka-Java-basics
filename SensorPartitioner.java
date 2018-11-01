package org.kafka.practice;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

public class SensorPartitioner implements Partitioner {

	private String sensorName;
	@Override
	public void configure(Map<String, ?> confs) {
		sensorName = confs.get("sensor.name").toString();
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

		//PartitionInfo class gives Information about a topic-partition.  variables => topic, partition, leader, replicas, inSyncReplicas.
		
		List<PartitionInfo> partitionsInfo = cluster.partitionsForTopic(topic);
		int numOfPartitions = partitionsInfo.size();
		int sp = (int)Math.abs(numOfPartitions*0.3) ;
		int p = 0;
		
		if ( (keyBytes == null) || (!(key instanceof String)) )
            throw new InvalidRecordException("All messages must have sensor name as key");
		
		if(key.equals(sensorName))
			//murmur2 is a hash algorithm in Utils class. Parameters => byte Array.  Returns => 32 bit hash.
			//toPositive() is method in Utils class. A cheap way to deterministically convert a number to a positive value.
			p = Utils.toPositive(Utils.murmur2(valueBytes)) % sp;
		else
			p = Utils.toPositive(Utils.murmur2(keyBytes)) % (numOfPartitions-sp) +sp;
		System.out.println("Topic:" + topic + "Key:" + key + " Partition:" +p);
		return p;
	}

	@Override
	public void close() {	}

}
