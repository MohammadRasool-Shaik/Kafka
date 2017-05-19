/**
 * 
 */
package org.rash.kafka.avro;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

/**
 * @author Rasool.Shaik
 *
 */
public class AvroConsumerWithParsersUsingBijection {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("group.id", "RG");
		props.put("enable.auto.commit", "false");

		Schema schema = new Schema.Parser().parse(new File("D:\\Workspace\\Kafka\\src\\main\\resources\\avro\\schema\\EmployeeV1.avsc"));

		Consumer<String, byte[]> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("AvroClicks"));

		try {
			Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
			while (true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(100);
				for (ConsumerRecord<String, byte[]> record : records) {
					GenericRecord genericRecord = recordInjection.invert(record.value()).get();
					System.out.println("Eid : " + genericRecord.get("eid") + " Employee  Name = " + genericRecord.get("name") + " salary : " + genericRecord.get("salary"));
				}
				consumer.commitAsync();
			}
		} finally {
			consumer.commitSync();
			consumer.close();
		}
	}
}
