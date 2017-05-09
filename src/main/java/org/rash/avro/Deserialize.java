/**
 * 
 */
package org.rash.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.rash.avro.dto.Employee;

/**
 * @author Rasool.Shaik
 *
 */
public class Deserialize {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// DeSerializing the objects
		DatumReader<Employee> empDatumReader = new SpecificDatumReader<Employee>(Employee.class);

		// Instantiating DataFileReader
		DataFileReader<Employee> dataFileReader = new DataFileReader<Employee>(new File("D:\\Workspace\\Kafka\\src\\main\\resources\\avro\\serializeddata\\emp.avro"), empDatumReader);
		Employee em = null;

		while (dataFileReader.hasNext()) {

			em = dataFileReader.next(em);
			System.out.println(em);
		}

		dataFileReader.close();

	}

}
