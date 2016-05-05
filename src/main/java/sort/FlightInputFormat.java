package sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.StringUtils;

public class FlightInputFormat extends FileInputFormat<Flight, NullWritable> {

	public class FlightRecordReader extends RecordReader<Flight, NullWritable> {
		private Text line = new Text();
		private Flight outputKey = new Flight();
		private final NullWritable nulVal = NullWritable.get();
		private LineReader in;
		long start, end, currentPos;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			FileSplit inputSplit = (FileSplit) split;
			Configuration config = context.getConfiguration();
			Path file = inputSplit.getPath();
			FSDataInputStream is = file.getFileSystem(config).open(file);
			in = new LineReader(is, config);
			start = inputSplit.getStart();
			end = start + inputSplit.getLength();
			is.seek(start);
			if (start != 0) {
				start += in.readLine(new Text(), 0, (int) Math.min(Integer.MAX_VALUE, end - start));
			}
			currentPos = start;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (currentPos > end) {
				return false;
			}
			currentPos += in.readLine(line);
			if (line.getLength() == 0) {
				return false;
			} else if (line.toString().startsWith("Year")) {
				currentPos += in.readLine(line);
			}
			String[] values = StringUtils.split(line.toString(), ',');
			if (values.length > 0) {
				outputKey.setDate(values[0].trim() + "-" + values[1].trim() + "-" + values[2].trim());
				if (org.apache.commons.lang3.StringUtils.isNumeric(values[14].trim())
						&& !org.apache.commons.lang3.StringUtils.isEmpty(values[14].trim()))
					outputKey.setArrDelay(values[14].trim());
				else
					outputKey.setArrDelay("0");
				return true;
			}
			return false;
		}

		@Override
		public Flight getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return outputKey;
		}

		@Override
		public NullWritable getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return nulVal;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			in.close();
		}

	}

	@Override
	public RecordReader<Flight, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new FlightRecordReader();
	}

}
