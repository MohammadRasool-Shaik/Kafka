/**
 * 
 */
package org.rash.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Rasool.Shaik 
 * Sensor Producer
 */
public class CustomPartitionerProducer {
	public static void main(String[] args) {
		String topicName = "SensorTopic";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("partitioner.class", "org.rash.kafka.producer.CustomPartitioner");
		props.put("customPropsKey", "TSS");
		props.put("acks", "all");
		props.put("retries", 0);

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 10; i++)
			producer.send(new ProducerRecord<>(topicName, "SSP" + i, "500" + i));

		for (int i = 0; i < 10; i++)
			producer.send(new ProducerRecord<>(topicName, "TSS", "500" + i));

		producer.close();
	}
}
