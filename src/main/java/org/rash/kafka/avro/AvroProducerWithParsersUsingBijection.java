/**
 * 
 */
package org.rash.kafka.avro;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

/**
 * @author Rasool.Shaik
 *
 */
public class AvroProducerWithParsersUsingBijection {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		final String topicName = "AvroClicks";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		Schema schema = new Schema.Parser().parse(new File("D:\\Workspace\\Kafka\\src\\main\\resources\\avro\\schema\\EmployeeV1.avsc"));

		Producer<String, byte[]> producer = new KafkaProducer<>(props);
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

		GenericRecord g1 = new GenericData.Record(schema);
		g1.put("eid", 1);
		g1.put("name", "one");
		g1.put("salary", 45000.00);
		g1.put("age", 26);
		g1.put("gender", "Male");
		g1.put("designation", "develeoper2");
		
		producer.send(new ProducerRecord<String, byte[]>(topicName, recordInjection.apply(g1)));

		
		GenericRecord g2 = new GenericData.Record(schema);
		g2.put("eid", 2);
		g2.put("name", "two");
		g2.put("salary", 60000.00);
		g2.put("age", 29);
		g2.put("gender", "Male");
		g2.put("designation", "develeoper1");
		
		producer.send(new ProducerRecord<String, byte[]>(topicName, recordInjection.apply(g2)));

		
		GenericRecord g3 = new GenericData.Record(schema);
		g3.put("eid", 3);
		g3.put("name", "three");
		g3.put("salary", 7500.00);
		g3.put("age", 24);
		g3.put("gender", "Male");
		g3.put("designation", "develeoper");
		
		producer.send(new ProducerRecord<String, byte[]>(topicName, recordInjection.apply(g3)));


		producer.close();
		System.out.println("Data successfully serialized pushed into kafka Broker");
	}

}
