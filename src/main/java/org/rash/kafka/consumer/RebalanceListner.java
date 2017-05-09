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
 * Responsibility of this class
 * 1) Maintain a list of offsets that are processed and ready to be committed. 
 *    i.e Consumer need to maintain list of offset instead of relying on the current offset that are maange by kafka
 * 2) Commit the offsets when partitions are going away.
 *
 * When new consumer added/crashed from the consumer group following things can happen
 * A rebalance will be triggered
 * Kafka will revoke all partitions,  onPartitionsRevoked() calls here 
 * New partition assignment, onPartitionsAssigned() calls here
 */
public class RebalanceListner implements ConsumerRebalanceListener {
	private Consumer<String, String> consumer;

	// To maintain the offsets, just keep the latest offset for the topic and the partition. 	
	private Map<TopicPartition, OffsetAndMetadata> currentOffSet = new HashMap<>();

	/**
	 * @param consumer
	 */
	public RebalanceListner(Consumer<String, String> consumer) {
		this.consumer = consumer;
	}

	public void addOffset(String topic, int partition, long offset) {
		currentOffSet.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "commit"));
	}

	/**
	 * @return the currentOffSet
	 */
	public Map<TopicPartition, OffsetAndMetadata> getCurrentOffSet() {
		return currentOffSet;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)
	 */
	/*
	 * The API will call this method, just before it takes away your partitions. Here You can commit your current offset.
	 */
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(java.util.Collection)
	 * 
	 * The API will call this method, right after completing Rebalance is complete and before you start consuming messages from the new partition.
	 */
	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

		System.out.println("Following partitions are assiagned");
		for (TopicPartition partition : partitions) {
			System.out.println(partition.partition() + ", ");
		}
	}

}
