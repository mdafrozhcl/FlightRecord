package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlightJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(super.getConf(), "flight job sort");
		Configuration config = job.getConfiguration();
		job.setJarByClass(getClass());

		Path in = new Path("app16/");
		Path out = new Path("app16/output");
		out.getFileSystem(config).delete(out, true);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setInputFormatClass(FlightInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(FlightMapper.class);
		job.setReducerClass(Reducer.class);
		job.setMapOutputKeyClass(Flight.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Flight.class);
		job.setOutputValueClass(NullWritable.class);
		job.setCombinerClass(Reducer.class);
		job.setGroupingComparatorClass(FlightGroupComparator.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new FlightJob(), args);
		System.exit(result);
	}
}
