package ejercicio2.Twitter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * @author Carlos López Moreno
 */

public class TweetOutputFormat extends FileOutputFormat<Tweet, TweetValues> {

	public class TweetRecordWriter extends RecordWriter<Tweet, TweetValues> {

		public final String SEPARATOR = ",";
		private DataOutputStream out;

		public TweetRecordWriter(DataOutputStream out){
			this.out=out;
		}

		public void write(Tweet key, TweetValues value)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringBuilder result = new StringBuilder();
			String volume_momentum = new DecimalFormat("#0.0000").format(value.getVolume_momentum());
			String popularity_momentum = new DecimalFormat("#0.0000").format(value.getPopularity_momentum());
			result.append(key.getSreen_name()+SEPARATOR+value.getTotal_tweets()+SEPARATOR+volume_momentum+SEPARATOR+popularity_momentum+"\n");
			out.write(result.toString().getBytes());
		}

		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			out.close();
		}
	}

	public RecordWriter<Tweet, TweetValues> getRecordWriter(
		TaskAttemptContext job) throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	
		int partition =	 job.getTaskAttemptID().getTaskID().getId();
		Path outputDir = FileOutputFormat.getOutputPath(job);
		Path file = new Path(outputDir.getName() + Path.SEPARATOR + "Estadisticas_Twitter_" + partition+".csv");
		FileSystem fs =	 file.getFileSystem(job.getConfiguration());
		FSDataOutputStream fileOut = fs.create(file);
		
		return new TweetRecordWriter(fileOut);
	
	}


}
