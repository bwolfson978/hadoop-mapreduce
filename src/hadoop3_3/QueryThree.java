package hadoop3_3;
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class QueryThree {
	
	public static class CustMapper
		extends Mapper<Object, Text, IntWritable, Text> {
		
		private final String filetag = "CD,";
	
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//populate the hash table here with
			String elems[] = value.toString().split(",");
			int custID = Integer.parseInt(elems[0]);
			Text result = new Text(filetag+elems[1]+","+elems[4]);
			context.write(new IntWritable(custID), result); 
		}
	}

	public static class TransMapper
		extends Mapper <Object, Text, IntWritable, Text> {
		private final String filetag = "TR,";

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//populate the hash table here with
			String elems[] = value.toString().split(",");
			int custID = Integer.parseInt(elems[1]);
			Text result = new Text(filetag+elems[2]+","+elems[3]);
			context.write(new IntWritable(custID), result);
		}
	}

	public static class QueryReducer
		extends Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int numTrans = 0;
			double total = 0;
			String salary = "NONE";
			int minItems = 10000000;
			String name = "NONE";
			for(Text val: values) {
				String elems[] = val.toString().split(",");
				if(elems[0].equals("CD")){
					// update the customer relevant values
					name=elems[1];
					salary = elems[2];
				} else if(elems[0].equals("TR")) {
					// update the transaction relevant values
					double value = Double.parseDouble(elems[1]);
					int numItems = Integer.parseInt(elems[2]);
					total += value;
					numTrans++;
					minItems = Math.min(minItems, numItems);
				}
			}
			String result = name 	+ "," + salary 
						+ "," + Integer.toString(numTrans)
						+ "," + Double.toString(total)
						+ "," + Integer.toString(minItems);
			context.write(key, new Text(result));
		}
	}

	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		Job j = Job.getInstance(conf, "Query Three");
		j.setJarByClass(QueryThree.class);
		j.setReducerClass(QueryReducer.class);

		j.setOutputKeyClass(IntWritable.class);
		j.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(j, new Path(args[0]), TextInputFormat.class, CustMapper.class);
		MultipleInputs.addInputPath(j, new Path(args[1]), TextInputFormat.class, TransMapper.class);
		

		FileOutputFormat.setOutputPath(j, new Path(args[2]));
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	

	

}
