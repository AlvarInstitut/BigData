package ejercicio1.Prices;

import java.io.DataOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * @author Carlos López Moreno
 */

public class StockOutputFormat extends FileOutputFormat<Stock, DoubleWritable> {

	public class StockRecordWriter extends RecordWriter<Stock, DoubleWritable> {

		public final String SEPARATOR = ",";
		private DataOutputStream out;

		public StockRecordWriter(DataOutputStream out){
			this.out=out;
		}

		public void write(Stock key, DoubleWritable value)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringBuilder result = new StringBuilder();
			String average = new DecimalFormat("#0.0000").format(value.get());
			
			
			result.append(key.toString()+SEPARATOR+average+"\n");
			out.write(result.toString().getBytes());
		}

		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			out.close();
		}
	}

	public RecordWriter<Stock, DoubleWritable> getRecordWriter(
		TaskAttemptContext job) throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	
		int partition =	 job.getTaskAttemptID().getTaskID().getId();
		Path outputDir = FileOutputFormat.getOutputPath(job);
		Path file = new Path(outputDir.getName() + Path.SEPARATOR + "Precios_medios_" + partition+".csv");
		FileSystem fs =	 file.getFileSystem(job.getConfiguration());
		FSDataOutputStream fileOut = fs.create(file);
		
		return new StockRecordWriter(fileOut);
	
	}


}

