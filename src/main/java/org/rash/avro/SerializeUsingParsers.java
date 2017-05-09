/**
 * 
 */
package org.rash.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

/**
 * @author Rasool.Shaik
 *
 */
public class SerializeUsingParsers {
	public static void main(String[] args) throws IOException {
		Schema schema = new Schema.Parser().parse(new File("D:\\Workspace\\Kafka\\src\\main\\resources\\avro\\schema\\EmployeeV1.avsc"));

		GenericRecord g1 = new GenericData.Record(schema);
		g1.put("eid", 1);
		g1.put("name", "one");
		g1.put("salary", 45000.00);
		g1.put("age", 26);
		g1.put("gender", "Male");
		g1.put("designation", "develeoper2");

		GenericRecord g2 = new GenericData.Record(schema);
		g2.put("eid", 2);
		g2.put("name", "two");
		g2.put("salary", 60000.00);
		g2.put("age", 29);
		g2.put("gender", "Male");
		g2.put("designation", "develeoper1");

		GenericRecord g3 = new GenericData.Record(schema);
		g3.put("eid", 3);
		g3.put("name", "three");
		g3.put("salary", 7500.00);
		g3.put("age", 24);
		g3.put("gender", "Male");
		g3.put("designation", "develeoper");

		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);

		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

		dataFileWriter.create(schema, new File("D:\\Workspace\\Kafka\\src\\main\\resources\\avro\\serializeddata\\empV1.avro"));
		dataFileWriter.append(g3);
		dataFileWriter.append(g2);
		dataFileWriter.append(g1);
		

		dataFileWriter.close();
		
		System.out.println("data successfully serialized");
	}
}
