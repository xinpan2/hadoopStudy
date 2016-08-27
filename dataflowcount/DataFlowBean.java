package dataflowcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * the class is the encapsulation of the user's phoneNum,upload flow,download
 * flow
 */
public class DataFlowBean implements Writable {
	private String phoneNum;
	private long upload_flow;
	private long download_flow;
	private long sum_flow;

	/**
	 * this method is used for write fields to the byte sequence by hadoop's own
	 * method
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(phoneNum);
		out.writeLong(upload_flow);
		out.writeLong(download_flow);
		out.writeLong(sum_flow);

	}

	/**
	 * this method is used for deserialization but the sequence of reading
	 * fields must equals with what have been wrote in the write method
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		phoneNum = in.readUTF();
		upload_flow = in.readLong();
		download_flow = in.readLong();
		sum_flow = in.readLong();
	}

	/**
	 * the method try to assign fields with 3 params instead of providing the
	 * constructor
	 * 
	 * @param phoneNum
	 * @param upload_flow
	 * @param download_flow
	 */
	public void set(String phoneNum, long upload_flow, long download_flow) {
		this.phoneNum = phoneNum;
		this.upload_flow = upload_flow;
		this.download_flow = download_flow;
		this.sum_flow = upload_flow + download_flow;
	}

	/**
	 * since the MR app has the step to ouput this object,we may override the
	 * toString method .Otherwise we will see a hashcode of the object in the
	 * result file!
	 * 
	 * cause the output key is the phoneNum,we omit the filed!
	 */
	@Override
	public String toString() {
		return +this.upload_flow + "\t" + this.download_flow + "\t" + this.sum_flow;
	}

	// getter setter

	public String getPhoneNum() {
		return phoneNum;
	}

	public void setPhoneNum(String phoneNum) {
		this.phoneNum = phoneNum;
	}

	public long getUpload_flow() {
		return upload_flow;
	}

	public void setUpload_flow(long upload_flow) {
		this.upload_flow = upload_flow;
	}

	public long getDownload_flow() {
		return download_flow;
	}

	public void setDownload_flow(long download_flow) {
		this.download_flow = download_flow;
	}

	public long getSum_flow() {
		return sum_flow;
	}

	public void setSum_flow(long sum_flow) {
		this.sum_flow = sum_flow;
	}

}
