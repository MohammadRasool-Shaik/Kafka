/**
 * 
 */
package org.rash.kafka.producer;

import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Rasool.Shaik
 *
 */
public class ListnerProducer {
	public static void main(String[] args) {
		final String topicName = "ListnerTopic";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("acks", "all");
        props.put("retries", 2);
		Random random = new Random();
		Producer<String, String> producer = new KafkaProducer<>(props);
		String msg = null;
		Calendar date = Calendar.getInstance();
		date.set(2017, 01, 01);

		try {
			while (true) {
				for (int i = 0; i < 100; i++) {
					msg = date.get(Calendar.YEAR) + "-" + date.get(Calendar.MONTH) + "-" + date.get(Calendar.DATE) + "-" + random.nextInt(1000);
					producer.send(new ProducerRecord<String, String>(topicName, 0, "key", msg));
					msg = date.get(Calendar.YEAR) + "-" + date.get(Calendar.MONTH) + "-" + date.get(Calendar.DATE) + "-" + random.nextInt(1000);
					producer.send(new ProducerRecord<String, String>(topicName, 1, "key", msg));
				}
				date.add(Calendar.DATE, 1);
				System.out.println("Data Sent for " + date.get(Calendar.YEAR) + "-" + date.get(Calendar.MONTH) + "-" + date.get(Calendar.DATE));
			}
		} finally {
			producer.close();
		}
	}
}
