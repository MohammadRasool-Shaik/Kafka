/**
 * 
 */
package org.rash.kafka.producer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.rash.avro.dto.Supplier;

/**
 * @author Rasool.Shaik
 *
 */
public class SupplierProducer {

	/**
	 * @param args
	 * @throws ParseException
	 */
	public static void main(String[] args) throws ParseException {
		String topicName = "SupplierTopic";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.rash.kafka.producer.SupplierSerializer");
		
		props.put("acks", "all");
        props.put("retries", 0);
		props.put("max.in.flight.requests.per.connection", "org.rash.kafka.producer.SupplierSerializer");

		Producer<String, Supplier> producer = new KafkaProducer<>(props);

		SimpleDateFormat sd = new SimpleDateFormat("dd-MM-yyyy");

		Supplier s1 = new Supplier(1, "Rasool", sd.parse("28-04-2017"));
		Supplier s2 = new Supplier(2, "Papi", sd.parse("18-04-2017"));

		ProducerRecord<String, Supplier> producerRecord = new ProducerRecord<String, Supplier>(topicName, "SUP", s1);
		ProducerRecord<String, Supplier> producerRecord1 = new ProducerRecord<String, Supplier>(topicName, "SUP", s2);
		try {
			Future<RecordMetadata> future = producer.send(producerRecord);
			RecordMetadata recordMetadata = future.get();
			System.out.println("OffSet " + recordMetadata.offset());

			producer.send(producerRecord1, (metaData, exception) -> {
				if (exception != null) {
					System.out.println(exception.getMessage());
				} else {
					System.out.println("TopicName " + metaData.topic());
				}
			});
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}

		producer.close();
	}

}
