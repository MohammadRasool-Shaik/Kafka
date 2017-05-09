/**
 * 
 */
package org.rash.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.rash.avro.dto.Employee;

/**
 * @author Rasool.Shaik
 *
 */
public class Serialize {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Employee emp = new Employee();
		emp.setEid(1);
		emp.setName("Rasool");
		emp.setAge(23);
		emp.setGender("Male");
		emp.setDesignation("Developer");

		Employee emp1 = new Employee();
		emp1.setEid(2);
		emp1.setName("Rasool1");
		emp1.setAge(25);
		emp1.setGender("Male");
		emp1.setDesignation("Developer1");

		Employee emp2 = new Employee();
		emp2.setEid(3);
		emp2.setName("Rasool2");
		emp2.setAge(28);
		emp2.setGender("Male");
		emp2.setDesignation("Developer2");

		Employee emp3 = new Employee();
		emp3.setEid(4);
		emp3.setName("Rasool3");
		emp3.setAge(29);
		emp3.setGender("Male");
		emp3.setDesignation("Developer3");

		DatumWriter<Employee> empDatumWriter = new SpecificDatumWriter<Employee>(Employee.class);
		DataFileWriter<Employee> empFileWriter = new DataFileWriter<Employee>(empDatumWriter);

		empFileWriter.create(emp.getSchema(), new File("D:\\Workspace\\Kafka\\src\\main\\resources\\avro\\serializeddata\\emp.avro"));

		empFileWriter.append(emp);
		empFileWriter.append(emp1);
		empFileWriter.append(emp2);
		empFileWriter.append(emp3);

		empFileWriter.close();

		System.out.println("data successfully serialized");
	}

}
