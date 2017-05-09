/**
 * 
 */
package org.rash.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Rasool.Shaik
 *
 */
public class SynchronousProducer {
	public static void main(String[] args) {
		String topicName = "SynchronousProducerTopic";
		String key = "key1";
		String value = "value1";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("acks", "all");
        props.put("retries", 0);

		Producer<String, String> producer = new KafkaProducer<>(props);

		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
		Future<RecordMetadata> send = producer.send(producerRecord);
		try {
			RecordMetadata recordMetadata = send.get();
			System.out.println(recordMetadata.topic() + " " + recordMetadata.partition() + " " + recordMetadata.offset() + " Synchronous Producer Created");
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}

}
