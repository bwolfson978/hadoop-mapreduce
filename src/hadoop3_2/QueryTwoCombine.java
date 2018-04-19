package hadoop3_2;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class QueryTwoCombine {
	
	public static class QueryMapper
			extends Mapper<Object, Text, IntWritable, DoubleWritable> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// extract all the transaction values and customerIDs
			String[] elems = value.toString().split(",");
			int custID = Integer.parseInt(elems[1]);
			double transValue = Double.parseDouble(elems[2]);
			context.write(new IntWritable(custID), new DoubleWritable(transValue));
		}

	}

	public static class QueryReducer
		extends Reducer<IntWritable, DoubleWritable, IntWritable, Text> {
		
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double total = 0;
			int count = 0;
			for(DoubleWritable val: values) {
				total += val.get();
				count ++;
			}
			Text result = new Text(Integer.toString(count) + ",  " + Double.toString(total));
			context.write(key, result);
		}
	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		Job j = Job.getInstance(conf, "Query Two");
		j.setJarByClass(QueryTwoCombine.class);
		j.setMapperClass(QueryMapper.class);
		//j.setCombinerClass(QueryReducer.class);
		j.setReducerClass(QueryReducer.class);


		j.setMapOutputKeyClass(IntWritable.class);
		j.setMapOutputValueClass(DoubleWritable.class);
		j.setOutputKeyClass(IntWritable.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	

	

}
