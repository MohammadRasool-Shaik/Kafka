/**
 * 
 */
package org.rash.kafka.consumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Rasool.Shaik
 *
 */
public class FullFeaturedListner implements ConsumerRebalanceListener {
	private Consumer<Integer, byte[]> consumer;

	private Map<TopicPartition, OffsetAndMetadata> currentOffSet = new HashMap<>();

	public FullFeaturedListner(Consumer<Integer, byte[]> consumer) {
		this.consumer = consumer;
	}

	public void addOffSet(String topic, int partition, long offset) {
		currentOffSet.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset));
	}

	/**
	 * @return the currentOffSet
	 */
	public Map<TopicPartition, OffsetAndMetadata> getCurrentOffSet() {
		return currentOffSet;
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		System.out.println("following partitions are revoked");
		for (TopicPartition partition : partitions) {
			System.out.println(partition.partition() + ", ");
		}
		System.out.println("following partitions are commited");

		for (TopicPartition topicPartition : currentOffSet.keySet()) {
			System.out.println(topicPartition.partition() + ", ");
		}
		consumer.commitSync(currentOffSet);
		currentOffSet.clear();
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		System.out.println("Following partitions are assiagned");
		for (TopicPartition partition : partitions) {
			System.out.println(partition.partition() + ", ");
		}
	}
}
