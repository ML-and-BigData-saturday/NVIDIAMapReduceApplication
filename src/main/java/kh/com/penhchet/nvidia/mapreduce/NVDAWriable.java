package kh.com.penhchet.nvidia.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class NVDAWriable implements Writable{
	
	private DoubleWritable max;
	private DoubleWritable average;
	
	public NVDAWriable(){
		this.max = new DoubleWritable();
		this.average = new DoubleWritable();
	}
	
	public NVDAWriable(DoubleWritable max, DoubleWritable average) {
		this.max = max;
		this.average = average;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		max.write(dataOutput);
		average.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		max.readFields(dataInput);
		max.readFields(dataInput);
	}

	public DoubleWritable getMax() {
		return max;
	}

	public void setMax(DoubleWritable max) {
		this.max = max;
	}

	public DoubleWritable getAverage() {
		return average;
	}

	public void setAverage(DoubleWritable average) {
		this.average = average;
	}

	@Override
	public String toString() {
		return max + " " + average;
	}
	
	

}
