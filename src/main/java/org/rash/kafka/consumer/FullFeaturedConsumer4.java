/**
 * 
 */
package org.rash.kafka.consumer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

/**
 * @author Rasool.Shaik
 *
 */
public class FullFeaturedConsumer4 {
	private static final String topicName = "FeaturedTopic";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		props.put("group.id", "groupTwo");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("enable.auto.commit", "false");

		Consumer<Integer, byte[]> consumer = null;
		try {
			consumer = new KafkaConsumer<>(props);
			FullFeaturedListner listner = new FullFeaturedListner(consumer);
			consumer.subscribe(Arrays.asList(topicName), listner);
			Schema schema = new Schema.Parser().parse(new File("D:\\Workspace\\Kafka\\src\\main\\resources\\avro\\schema\\CityV1.avsc"));
			Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
			WritingintoFile writeToFile = WritingintoFile.getInstance();
			List<String> lines = new ArrayList<>();
			while (true) {
				ConsumerRecords<Integer, byte[]> consumerRecords = consumer.poll(100);
				if (!consumerRecords.isEmpty()) {
					for (ConsumerRecord<Integer, byte[]> consumerRecord : consumerRecords) {
						GenericRecord genericRecord = recordInjection.invert(consumerRecord.value()).get();
						lines.add("id: " + genericRecord.get("id") + " name: " + genericRecord.get("name") + " countryCode: " + genericRecord.get("countryCode") + " district: "
								+ genericRecord.get("district") + " population: " + genericRecord.get("population"));
						System.out.println("id: " + genericRecord.get("id") + " name: " + genericRecord.get("name") + " countryCode: " + genericRecord.get("countryCode")
								+ " district: " + genericRecord.get("district") + " population: " + genericRecord.get("population"));
						listner.addOffSet(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
					}
					writeToFile.writeToFile(lines, "four");
					Map<TopicPartition, OffsetAndMetadata> currentOffSet = listner.getCurrentOffSet();
					consumer.commitSync(currentOffSet);
					currentOffSet.clear();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}
