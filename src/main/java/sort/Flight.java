package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Flight implements WritableComparable<Flight> {

	private String date;
	private String arrDelay;

	Flight() {
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(String arrDelay) {
		this.arrDelay = arrDelay;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(date);
		out.writeUTF(arrDelay);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		date = in.readUTF();
		arrDelay = in.readUTF();
	}

	@Override
	public int compareTo(Flight o) {
		int result = this.getDate().compareTo(o.getDate());
		if (result == 0) {
			result = (-1) * (Integer.parseInt(this.getArrDelay()) - Integer.parseInt(o.getArrDelay()));
		}
		return result;
	}

	@Override
	public String toString() {
		return date + "," + arrDelay;
	}
}
