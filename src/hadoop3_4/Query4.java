package hadoop3_4;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query4 {
	
	public static class Query4Mapper 
		extends Mapper<Object, Text, IntWritable, CompositeWritable> {

		private HashMap<Integer, Pair> map = new HashMap<Integer, Pair>();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			try{
				Path[] customerFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				System.out.println("got HERE");
				if(customerFiles != null && customerFiles.length > 0){
					for(Path p : customerFiles){
						System.out.println("getting here");
						readFile(p);
					}
				}
			}catch(IOException e){
				System.err.println("setup exception: " + e.getMessage());
			}
		}
		
		private void readFile(Path p){
			try{
				BufferedReader br = new BufferedReader(new FileReader(p.toString()));
				String line = null;
				while((line = br.readLine()) != null){
					String[] elems = line.split(",");
					int custId = Integer.parseInt(elems[0]);
					int ccode = Integer.parseInt(elems[3]);
					int seen = 0;
					Pair pair = new Pair(ccode, seen);
					map.put(custId, pair);
				}
			}catch(IOException e){
				System.err.println("Exception reading cache" + e.getMessage());
			}
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer tkns = new StringTokenizer(value.toString(), "\n");
			while(tkns.hasMoreTokens()) {
				String trans = tkns.nextToken();
				String[] elems = trans.split(",");
				int custId = Integer.parseInt(elems[1]);
				float tVal = Float.parseFloat(elems[2]);
				//check if seen this customer already
				Pair p = map.get(custId);
				int ccode = p.getCcode();
				IntWritable k = new IntWritable(ccode);
				FloatWritable transVal = new FloatWritable(tVal);
				if(p.getSeen() == 1){
					IntWritable notSeen = new IntWritable(0);
					CompositeWritable c = new CompositeWritable(notSeen, transVal);
					context.write(k, c);
				}
				else if(p.getSeen() == 0){
					//update HashMap
					Pair newPair = new Pair(ccode, 1);
					map.put(custId, newPair);
					IntWritable notSeen = new IntWritable(1);
					CompositeWritable c = new CompositeWritable(notSeen, transVal);
					context.write(k, c);
				}
			}
		}
	}

	public static class Query4Reducer
		extends Reducer<IntWritable, CompositeWritable, IntWritable, Text>{
		
		//private IntWritable cust = new IntWritable(0);
		//private FloatWritable minVal = new FloatWritable(0);
		//private FloatWritable maxVal = new FloatWritable(0);
		
		public void reduce(IntWritable key, Iterable<CompositeWritable> values, Context context)
			throws IOException, InterruptedException {
			
			int numCust = 0;
			float minTransVal = Float.MAX_VALUE;
			float maxTransVal = Float.MIN_VALUE;
			
			for(CompositeWritable arr : values){
				int seen = arr.getFreq().get();
				float transVal = arr.getTransVal().get();
				numCust += seen;
				if(transVal < minTransVal){
					minTransVal = transVal;
				}
				else if(transVal > maxTransVal){
					maxTransVal = transVal;
				}
			}
			
			String str = Integer.toString(numCust) + ", " +
						 Float.toString(minTransVal) + ", " +
					     Float.toString(maxTransVal);
			context.write(key, new Text(str));
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job j = Job.getInstance(conf, "Min/Max Trans by country code");
		DistributedCache.addCacheFile(new Path("/customers.csv").toUri(), j.getConfiguration());
		j.setJarByClass(Query4.class);
		j.setMapperClass(Query4Mapper.class);
		j.setReducerClass(Query4Reducer.class);
		j.setOutputKeyClass(IntWritable.class);
		j.setOutputValueClass(CompositeWritable.class);
		FileInputFormat.setInputPaths(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}
}

