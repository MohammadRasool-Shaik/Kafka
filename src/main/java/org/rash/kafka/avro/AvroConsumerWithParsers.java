/**
 * 
 */
package org.rash.kafka.avro;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author Rasool.Shaik
 *
 */
public class AvroConsumerWithParsers {

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("group.id", "RG");

		Schema schema = new Schema.Parser().parse(new File("D:\\Workspace\\Kafka\\src\\main\\resources\\avro\\schema\\EmployeeV1.avsc"));

		Consumer<String, byte[]> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("AvroClicks"));

		try {

			while (true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(100);
				DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
				for (ConsumerRecord<String, byte[]> record : records) {
					BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
					GenericRecord genericRecord = datumReader.read(null, decoder);
					System.out.println("Eid : " + genericRecord.get("eid") + " Employee  Name = " + genericRecord.get("name") + " salary : " + genericRecord.get("salary"));
				}
				consumer.commitAsync();
			}
		} finally {
			consumer.commitSync();
			consumer.close();
		}
		// ByteArrayInputStream inputStream = new ByteArrayInputStream(record.value());
		// ObjectInputStream os = new ObjectInputStream(inputStream);
		// System.out.println(os.readObject());
	}
}
