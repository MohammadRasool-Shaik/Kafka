/**
 * 
 */
package org.rash.kafka.avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
/**
 * @author Rasool.Shaik
 *
 */
public class AvroProducerWithParsers {

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

		Producer<String, byte[]> producer = new KafkaProducer<>(props);
		Schema schema = new Schema.Parser().parse(new File("D:\\Workspace\\Kafka\\src\\main\\resources\\avro\\schema\\EmployeeV1.avsc"));
		GenericRecord g1 = new GenericData.Record(schema);
		g1.put("eid", 1);
		g1.put("name", "one");
		g1.put("salary", 45000.00);
		g1.put("age", 26);
		g1.put("gender", "Male");
		g1.put("designation", "develeoper2");
		
		DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<GenericRecord>(schema);
		
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
		
		datumWriter.write(g1, encoder);
		
		byte[] byteArray = byteArrayOutputStream.toByteArray();
		
		producer.send(new ProducerRecord<String, byte[]>(topicName, "employee", byteArray));
		
		encoder.flush();
		
		byteArrayOutputStream.close();

		producer.close();
		System.out.println("Data successfully serialized pushed into kafka Broker");
	}
}
