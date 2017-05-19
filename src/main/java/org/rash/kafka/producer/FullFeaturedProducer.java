/**
 * 
 */
package org.rash.kafka.producer;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

/**
 * @author Rasool.Shaik
 *
 */
public class FullFeaturedProducer {

	private static final String topicName = "FeaturedTopic";

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		// TODO: Custom Partitinor needs to add to make full-featured producer
		// props.put("partitioner.class", "org.rash.kafka.producer.CustomPartitioner");
		// props.put("customPropsKey", "value you want to send"); //This value u can access at partitioner or serialize classes
		props.put("acks", "all");
		props.put("retries", 3);
		props.put("batch.size", 50); // Partition buffer to send batch of records
		props.put("linger.ms", 5); // 5ms, By default a buffer is available to send immediately even if there is additional unused space in the buffer. i.e 1ms
		props.put("max.in.flight.requests.per.connection", "1");
		Producer<Integer, byte[]> producer = new KafkaProducer<>(props);

		try {
			final Schema schema = new Schema.Parser().parse(new File("D:\\Workspace\\Kafka\\src\\main\\resources\\avro\\schema\\CityV1.avsc"));
			sendMessagestoKafka(producer, schema);
		} catch (ClassNotFoundException | SQLException | IOException e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}

	public static class MyProducerCallBack implements Callback {
		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
			if (exception != null) {
				exception.printStackTrace(System.out);
				System.out.println("Exception occures " + exception.getMessage());
			} else {
				System.out.println(recordMetadata.topic() + " " + recordMetadata.partition() + " " + recordMetadata.offset() + " Record log committed Successfully in broker");
			}
		}
	}

	public static void sendMessagestoKafka(Producer<Integer, byte[]> producer, Schema schema) throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/world", "root", "welcome");
		Statement statement = connection.createStatement();
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		statement.setFetchSize(200);
		ResultSet rs = statement.executeQuery("select * from city");
		while (rs.next()) {
			GenericRecord genericRecord = serializeUsingAvro(rs, schema);
			ProducerRecord<Integer, byte[]> producerRecord = new ProducerRecord<Integer, byte[]>(topicName, rs.getInt("ID"), recordInjection.apply(genericRecord));
			producer.send(producerRecord, new MyProducerCallBack());

		}
		rs.close();
		statement.close();
		connection.close();
	}

	public static GenericRecord serializeUsingAvro(ResultSet rs, Schema schema) throws SQLException {
		GenericRecord genericRecord = new GenericData.Record(schema);
		genericRecord.put("id", rs.getInt("ID"));
		genericRecord.put("name", rs.getString("Name"));
		genericRecord.put("countryCode", rs.getString("CountryCode"));
		genericRecord.put("district", rs.getString("District"));
		genericRecord.put("population", rs.getInt("Population"));

		return genericRecord;
	}

}
