/**
 * 
 */
package org.rash.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

/**
 * @author Rasool.Shaik
 *
 */
public class DeserializeUsingParsers {
	public static void main(String[] args) throws IOException {
		Schema schema = new Schema.Parser().parse(new File("D:\\Workspace\\Kafka\\src\\main\\resources\\avro\\schema\\EmployeeV1.avsc"));

		DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
		FileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File("D:\\Workspace\\Kafka\\src\\main\\resources\\avro\\serializeddata\\empV1.avro"), datumReader);
		while (dataFileReader.hasNext()) {
			System.out.println(dataFileReader.next());
		}
		dataFileReader.close();
		System.out.println("Data Readed successfully");
	}
}
