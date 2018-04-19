package hadoop3_4;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class CompositeWritable implements Writable{
	
	private IntWritable freq;
	private FloatWritable transVal;
	
	public CompositeWritable(){
		this.freq = new IntWritable(0);
		this.transVal = new FloatWritable(0);
	}
	
	public CompositeWritable(IntWritable freq, FloatWritable transVal){
		this.freq = freq;
		this.transVal = transVal;
	}
	
	public IntWritable getFreq(){
		return this.freq;
	}
	
	public void setFreq(IntWritable freq){
		this.freq = freq;
	}
	
	public FloatWritable getTransVal(){
		return this.transVal;
	}
	
	public void setTransVal(FloatWritable transVal){
		this.transVal = transVal;
	}
	
	public void readFields(DataInput in) throws IOException {
		freq.readFields(in);
		transVal.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException {
		freq.write(out);
		transVal.write(out);
	}
	
	public String toString(){
		return freq.toString() + " " + transVal.toString();
	}
	
	
}
