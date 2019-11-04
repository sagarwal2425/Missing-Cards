import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MissingCards {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Find Missing Poker Cards");
		job.setJarByClass(MissingCards.class);
		job.setMapperClass(pokerMapper.class);
		job.setReducerClass(pokerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class pokerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] rowSplit = (value.toString()).split("--");
			context.write(new Text(rowSplit[0]), new IntWritable(Integer.parseInt(rowSplit[1])));
		}
	}

	public static class pokerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> value, Context context)
				throws IOException, InterruptedException {
			ArrayList<Integer> nums = new ArrayList<Integer>();
			int add = 0;
			int tVal = 0;
			int index;
			for (IntWritable val : value) {
				add = add + val.get();
				tVal = val.get();
				nums.add(tVal);
			}
			if (add <= 90) {
				for (index = 1; index < 14; index++) {
					if (!nums.contains(index))
						context.write(key, new IntWritable(index));
				}
			}
		}
	}
}