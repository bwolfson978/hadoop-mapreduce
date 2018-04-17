import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountryCode {
	
	public static class CodeMapper 
		extends Mapper<Object, Text, IntWriteable, IntWritable> {

		public void map(Object key, Text value, Context context) {
			StringTokenizer tkns = new StringTokenizer(value.toString(), "\n");
			while(tkns.hasMoreTokens()) {
				String cust = tkns.nextToken();
				String[] elems = cust.split(",");
				int ccode = Integer.parseInt(elems[3]);
				int custId = Integer.parseInt(elems[0]);
				if (2 <= ccode && ccode <= 6) {
					context.write(new IntWritable(custId), new IntWritable(ccode));
				}
			}
				
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job j = Job.getInstance(conf, "Country Codes");
		j.setJarByClass(CounryCode.class);
		j.setMapperClass(CodeMapper.class);
		j.setOutputKeyClass(IntWritable.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, new Path(args[0]);
		FileOutputFormat.addOutputPath(j, new Path(args[1]);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}
}

