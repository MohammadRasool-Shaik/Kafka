/**
 * 
 */
package org.rash.avro.dto;

import java.util.Date;

/**
 * @author Rasool.Shaik
 *
 */
public class Supplier {
	private int supplierId;
	private String supplierName;
	private Date supplierStartDate;

	/**
	 * @param supplierId
	 * @param supplierName
	 * @param supplierStartDate
	 */
	public Supplier(int supplierId, String supplierName, Date supplierStartDate) {
		super();
		this.supplierId = supplierId;
		this.supplierName = supplierName;
		this.supplierStartDate = supplierStartDate;
	}

	/**
	 * @return the supplierId
	 */
	public int getSupplierId() {
		return supplierId;
	}

	/**
	 * @return the supplierName
	 */
	public String getSupplierName() {
		return supplierName;
	}

	/**
	 * @return the supplierStartDate
	 */
	public Date getSupplierStartDate() {
		return supplierStartDate;
	}

}
