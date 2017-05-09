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
 *
 */
public class SimpleProducer {
	public static void main(String[] args) {
		String topicName = "SimpleProducerTopic";
		String key = "key1";
		String value = "value1";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);

		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
		producer.send(producerRecord);
		producer.close();
		System.out.println("Simple Producer Created");
	}

}
