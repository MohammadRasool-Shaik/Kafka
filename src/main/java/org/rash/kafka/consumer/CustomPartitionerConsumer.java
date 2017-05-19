/**
 * 
 */
package org.rash.kafka.consumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Rasool.Shaik Sensor Consumer
 */
public class CustomPartitionerConsumer {
	public static void main(String[] args) {
		final String topicName = "SensorTopic";
		Properties props = new Properties();
		props.put("bootstrap.server", "localhost:9092,localhost:9093,localhost:9094");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeSerializer");
		props.put("enable.auto.commit", "false"); // this property will disable auto-commit feature and the consumer will not commit offset automatically by kafka broker.

		Consumer<String, String> consumer = new KafkaConsumer<>(props);

		// If you want Automatic group management and partition assignment. we should subscribe to the topic.
		// But in this example, we don't want kafka to assign partition to consumers. So we created as below  
		TopicPartition p0 = new TopicPartition(topicName, 0);
		TopicPartition p1 = new TopicPartition(topicName, 1);
		TopicPartition p2 = new TopicPartition(topicName, 2);

		// we self assign 
		consumer.assign(Arrays.asList(p0, p1, p2));
		System.out.println("Current position p0=" + consumer.position(p0) + " p1=" + consumer.position(p1) + " p2=" + consumer.position(p2));
		
		//Here we will set the offset position for 3 partitions. Here we are reading offset form db and we are adjusting offset at appropriate positions
		consumer.seek(p0, getOffsetFromDB(p0));
		consumer.seek(p1, getOffsetFromDB(p1));
		consumer.seek(p2, getOffsetFromDB(p2));
		System.out.println("New positions po=" + consumer.position(p0) + " p1=" + consumer.position(p1) + " p2=" + consumer.position(p2));
		System.out.println("Start Fetching Now");
		int rCount;
		try {
			do {
				//polling messages
				ConsumerRecords<String, String> records = consumer.poll(1000);
				System.out.println("Record polled " + records.count());
				rCount = records.count();
				//processing each messages and saving them in db at the same time we are updating offset for each message in db itself. so this will become an atomic trasaction operations(both processing record and saving offset)
				for (ConsumerRecord<String, String> record : records) {
					saveAndCommit(consumer, record);
				}
			} while (rCount > 0);
		} finally {
			consumer.close();
		}
	}

	private static long getOffsetFromDB(TopicPartition partition) {
		long offset = 0;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "welcome");

			String sql = "select offset from tss_offsets where topic_name='" + partition.topic() + "' and partition=" + partition.partition();
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next())
				offset = rs.getInt("offset");
			stmt.close();
			con.close();
		} catch (Exception e) {
			System.out.println("Exception in getOffsetFromDB");
		}
		return offset;
	}

	private static void saveAndCommit(Consumer<String, String> consumer, ConsumerRecord<String, String> record) {
		System.out.println("Topic=" + record.topic() + " Partition=" + record.partition() + " Offset=" + record.offset() + " Key=" + record.key() + " Value=" + record.value());
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "pandey");
			con.setAutoCommit(false);

			String insertSQL = "insert into tss_data values(?,?)";
			PreparedStatement psInsert = con.prepareStatement(insertSQL);
			psInsert.setString(1, record.key());
			psInsert.setString(2, record.value());

			String updateSQL = "update tss_offsets set offset=? where topic_name=? and partition=?";
			PreparedStatement psUpdate = con.prepareStatement(updateSQL);
			psUpdate.setLong(1, record.offset() + 1);
			psUpdate.setString(2, record.topic());
			psUpdate.setInt(3, record.partition());

			psInsert.executeUpdate();
			psUpdate.executeUpdate();
			con.commit();
			con.close();
		} catch (Exception e) {
			System.out.println("Exception in saveAndCommit");
		}

	}
}
