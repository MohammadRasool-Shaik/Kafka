/**
 * 
 */
package org.rash.kafka.consumer;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Rasool.Shaik
 *
 */
public class ListnerConsumer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("bootstrap.server", "localhost:9092,localhost:9093,localhost:9094");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeSerializer");
		props.put("group.id", "RG");

		Consumer<String, String> consumer = null;

		try {
			consumer = new KafkaConsumer<>(props);
			RebalanceListner rebalanceListner = new RebalanceListner(consumer);
			// We make sure that kafka will invoke the listener onPartitionRevoked().
			consumer.subscribe(Arrays.asList("ListnerTopic"), rebalanceListner);
			while (true) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					// consumerRecord.value();

					// Below 2 operations are not atomic. It is possible that save the record in db and my consumer got crashed before
					// committing the offset. So the re-balance listener will allow us to perform cleanup and commit before the partition goes away but
					// it cannot help us in syncing processed message and committing offset. There is otherway other than making Atomic transaction for both of the
					// operations. For this We don't want kafka to store the offset, So current and committed offset info also we should maintain in db.
					// As mention in CustomPartitionerConsumer example

					// Process those records and save into database
					// Maintain a list of offsets that are processed and ready to be committed.
					rebalanceListner.addOffset(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
				}
				Map<TopicPartition, OffsetAndMetadata> currentOffSet = rebalanceListner.getCurrentOffSet();
				// Once you finish processing all of the messages and ready to make next poll(), you should commit the offsets and reset the list
				consumer.commitSync(currentOffSet);
				currentOffSet.clear();
			}
		} finally {
			consumer.close();
		}
	}

}
