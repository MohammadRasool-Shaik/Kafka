/**
 * 
 */
package org.rash.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.rash.avro.dto.Supplier;

/**
 * @author Rasool.Shaik
 *
 */
/*
 * In this example, we are manually committing current offset, before polling the next set of records.
 * This example doesn't solve the problem completely, we are not committing at the current offset. We are processing set of messages, after that we are committing
 * If re-balance happen in middle or if we get any exception in b/w while processing messages we can get problem(Order of message will lost) for processing messages
 * We can get resolve above problems, if you know how to commit particular offset, instead of current offset.  
 */
public class SupplierConsumer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final String topicName = "SupplierTopic";
		final String consumerGroupName = "SupplierTopicGroup";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		props.put("group.id", consumerGroupName);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.rash.kafka.producer.SupplierDeserializer");

		Consumer<String, Supplier> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topicName));
		try {
			while (true) {
				// poll method return messages, poll(timeout): takes care of below things
				// connect to the group coordinator
				// Joint the group
				// Receives partition assignment
				// Sends heartbeat
				// fetches you messages
				ConsumerRecords<String, Supplier> records = consumer.poll(100);
				for (ConsumerRecord<String, Supplier> record : records) {
					System.out.println("Supplier id= " + String.valueOf(record.value().getSupplierId()) + " Supplier  Name = " + record.value().getSupplierName()
							+ " Supplier Start Date = " + record.value().getSupplierStartDate().toString());
				}
				consumer.commitAsync();
			}
		} finally {
			consumer.commitSync();
			consumer.close();
		}

	}

}
