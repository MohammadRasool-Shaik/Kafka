/**
 * 
 */
package org.rash.kafka.producer;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.rash.avro.dto.Supplier;

/**
 * @author Rasool.Shaik
 *
 */
public class SupplierSerializer implements Serializer<Supplier> {
	private static final String ENCODING = "UTF8";

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public byte[] serialize(String topic, Supplier data) {
		int sizeOfName;
		int sizeOfDate;
		byte[] serializedName;
		byte[] serializedDate;

		try {
			if (data == null)
				return null;
			serializedName = data.getSupplierName().getBytes(ENCODING);
			sizeOfName = serializedName.length;
			serializedDate = data.getSupplierStartDate().toString().getBytes(ENCODING);
			sizeOfDate = serializedDate.length;

			ByteBuffer buf = ByteBuffer.allocate(4 + 4 + sizeOfName + 4 + sizeOfDate);
			buf.putInt(data.getSupplierId());
			buf.putInt(sizeOfName);
			buf.put(serializedName);
			buf.putInt(sizeOfDate);
			buf.put(serializedDate);

			return buf.array();

		} catch (Exception e) {
			throw new SerializationException("Error when serializing Supplier to byte[]");
		}
	}

	@Override
	public void close() {
	}

}
