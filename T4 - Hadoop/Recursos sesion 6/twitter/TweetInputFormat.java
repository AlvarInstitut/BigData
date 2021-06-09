package ejercicio2.Twitter;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/*
 * @autho Carlos López Moreno
 */

public class TweetInputFormat extends FileInputFormat<Tweet, TweetValues> {
	
	public class TweetReader extends RecordReader<Tweet, TweetValues> {

		private Tweet key = new Tweet();
		private TweetValues value = new TweetValues();
		private LineReader in;
		private long start;
		private long end;
		private long currentPos;
		private Text line = new Text();
		
		
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
		
			FileSplit fileSplit = (FileSplit) split;

			Configuration conf = context.getConfiguration();
			Path path = fileSplit.getPath();
			FSDataInputStream is = path.getFileSystem(conf).open(path);
			in = new LineReader(is, conf);
			start = fileSplit.getStart();
			end = start + fileSplit.getLength();
			is.seek(start);
			if(start != 0){
				start += in.readLine(new Text(), 0, (int)Math.min(Integer.MAX_VALUE, end - start));
			}
			currentPos = start;
	
		}

		public boolean nextKeyValue() throws IOException, InterruptedException {
			ObjectMapper mapper = new ObjectMapper();
			
			if(currentPos > end){
				return false;
			}
			currentPos += in.readLine(line);
			if(line.getLength() == 0){
				return false;
			}
			if(line.toString().startsWith("exchange")){
				currentPos += in.readLine(line);
			}
			
			//Leemos una línea del fichero
			String values = line.toString();
			
			//sacamos la estructura json de "tweet"
			Map<String, Object> tweet = mapper.readValue(values, new TypeReference<Map<String, Object>>(){});
			@SuppressWarnings("unchecked")
			
			//de la estructura json del tweet sacamos la correspondiente a "user"
			Map<String, Object> user = (Map<String, Object>) tweet.get("user");
			
			//ponemos en la clave el nombre de usuario
			key.setScreen_name(String.valueOf(user.get("screen_name")));
			
			try {
				//vamos a formatear la fecha
				final String TWITTER="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
				SimpleDateFormat sf = new SimpleDateFormat(TWITTER,Locale.ENGLISH);
				sf.setLenient(true);
				String cadenaHora = String.valueOf(tweet.get("created_at"));
				
				Date fecha = sf.parse(cadenaHora);
		        String timeStamp = String.valueOf(fecha.getTime());
		        //pasamos la fecha formateada
				value.setCreated_at(timeStamp);
				
				//se añade a la clave para el ordenamiento por fecha
				key.setTime_stamp(fecha.getTime());
				
				//asignamos el número de retweets
				value.setRetweet_count(Integer.valueOf(String.valueOf(tweet.get("retweet_count"))));
				
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return true;
		}

		public Tweet getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return key;
		}

		public TweetValues getCurrentValue() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return value;
		}

		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		public void close() throws IOException {
			// TODO Auto-generated method stub
			in.close();
		}

	}

	@Override
	public RecordReader<Tweet, TweetValues> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		return new TweetReader(); 
		
	}
	


}
