package sort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<Flight, NullWritable, Flight, NullWritable> {
	private NullWritable nulval = NullWritable.get();

	@Override
	protected void map(Flight key, NullWritable value, Context context) throws IOException, InterruptedException {
		context.write(key, nulval);
	}
}
