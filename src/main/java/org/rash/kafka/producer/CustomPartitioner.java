/**
 * 
 */
package org.rash.kafka.producer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

/**
 * @author Rasool.Shaik
 * Sensor Partitioner
 */
public class CustomPartitioner implements Partitioner {

	private String speedSensorName;

	@Override
	public void configure(Map<String, ?> configs) {
		this.speedSensorName = configs.get("customPropsKey").toString();
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		List<PartitionInfo> partitionsForTopic = cluster.partitionsForTopic(topic);
		int totalNumberOfPartitions = partitionsForTopic.size();
		System.out.println("totalNumberOfPartitions " + cluster.partitionCountForTopic(topic) + " " + totalNumberOfPartitions);
		int sp = (int) Math.abs(totalNumberOfPartitions * 0.3);
		int p = 0;
		if (keyBytes == null || !(key instanceof String)) {
			throw new InvalidRecordException("All messages must have sensor name as key");
		}
		if (((String) key).equals(speedSensorName))
			p = Math.abs(Utils.murmur2(valueBytes)) % sp;
		else
			p = Math.abs(Utils.murmur2(keyBytes)) % (totalNumberOfPartitions - sp) + sp;
		System.out.println("Key = " + (String) key + " Partition = " + p);
		return p;
	}

	public static void main(String[] args) {
		String[] a = new String[8];
		List<String> stringList = Arrays.asList("one", "two", "one", "three", "four", "five");
		int totalLengthOfArray = a.length;
		int sp = (int) Math.abs((totalLengthOfArray * 0.3));
		int p = 0;

		for (String myString : stringList) {
			if (myString.equals("one") || myString.endsWith("four")) {
				p = Math.abs(myString.hashCode()) % sp;
			} else {
				p = Math.abs(myString.hashCode()) % (totalLengthOfArray - sp) + sp;
			}
			a[p] = myString;
		}

	}

	@Override
	public void close() {
	}
}
