package hadoop3_2;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AllCustomers {
	
	public static class CustomerMapper 
		extends Mapper<Object, Text, IntWritable, CompositeWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer tkns = new StringTokenizer(value.toString(), "\n");
			while(tkns.hasMoreTokens()) {
				String trans = tkns.nextToken();
				String[] elems = trans.split(",");
				int custId = Integer.parseInt(elems[1]);
				IntWritable one = new IntWritable(1);
				FloatWritable transVal = new FloatWritable(Float.parseFloat(elems[2]));
				CompositeWritable c = new CompositeWritable(one, transVal);
				context.write(new IntWritable(custId), c);
			}
				
		}
	}

	public static class CustomerReducer
		extends Reducer<IntWritable, CompositeWritable, IntWritable, CompositeWritable>{
		
		private IntWritable trans = new IntWritable(0);
		private FloatWritable transVal = new FloatWritable(0);
		
		public void reduce(IntWritable key, Iterable<CompositeWritable> values, Context context)
			throws IOException, InterruptedException {
			
			int numTrans = 0;
			float totalTransVal = 0;
			for(CompositeWritable arr : values){
				int one = arr.getFreq().get();
				float transVal = arr.getTransVal().get();
				numTrans += one;
				totalTransVal += transVal;
			}
			
			trans.set(numTrans);
			transVal.set(totalTransVal);
			CompositeWritable c = new CompositeWritable(trans, transVal);
			context.write(key, c);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job j = Job.getInstance(conf, "All Customers");
		j.setJarByClass(AllCustomers.class);
		j.setMapperClass(CustomerMapper.class);
		j.setCombinerClass(CustomerReducer.class);
		j.setOutputKeyClass(IntWritable.class);
		j.setOutputValueClass(CompositeWritable.class);
		FileInputFormat.setInputPaths(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}
}

