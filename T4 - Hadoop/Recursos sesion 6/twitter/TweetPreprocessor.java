package ejercicio2.Twitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * @author Carlos López Moreno 
 */
public class TweetPreprocessor extends Configured implements Tool {

	public static class PreprocessorMapper extends Mapper<Tweet, TweetValues, Tweet, TweetValues> {
		
		private TweetValues outputValue = new TweetValues();
		
		@Override
		protected void map(Tweet key, TweetValues values, Context context)
				throws IOException, InterruptedException {
			
			outputValue.setCreated_at(values.getCreated_at());
			outputValue.setRetweet_count(values.getRetweet_count());
			context.write(key, outputValue);
		}
		
		
	}
	
	public static class TweetReducer extends Reducer<Tweet, TweetValues, Tweet, TweetValues> {
		private NullWritable outputKey = NullWritable.get();
		private TweetValues outputValue = new TweetValues();
		
		protected void reduce(Tweet key, Iterable<TweetValues> values, Context context)
				throws IOException, InterruptedException {
			
			
			//declaración de variables
			int totalRetweets=0;
			int total_tweets=0;
			int N = 20;
			int limInf = 0;
			int limMed = 0;
			int mitad= N/2;
			double num_retweets=0;
			double volume_momentum=0.0;
			double popularity_momentum=0.0;
			long t_N=0;
			long t_0=0;
			long t_N_2=0;
			
			
			HashMap<Long,Integer> tweet_map = new HashMap<Long,Integer>();
			//Creamos una lista donde ir almacenando los timestamps de cada tweet 
			List<Long> t_timestamp=new ArrayList<Long>();
			
			//Creamos una lista para ir almacenando el número de retweets
			List<Integer> t_retweets=new ArrayList<Integer>();
			
		
			for(TweetValues value : values) {
				total_tweets++;
				t_timestamp.add(Long.valueOf(value.getCreated_at()));
				t_retweets.add(value.getRetweet_count());
				tweet_map.put(Long.valueOf(value.getCreated_at()), value.getRetweet_count());
				
			}
			
			
			if(total_tweets>1){
				
				if(N>total_tweets){ //si no tenemos un mínimo de N tweets, pues hacemos los cálculos con los que tengamos
					N=total_tweets;
					mitad = N/2;
					limInf = 0;
					limMed = N-mitad;
				}else{//sino calculamos los límites en base a la N establecida = 20
					limInf = (total_tweets-N);
					limMed = (total_tweets-mitad);
				}
				//Aplicamos la fórmula para Volume_momentum [(t_N - t_0) - (t_N - t_N/2)]/(t_N - t_0)
				t_N = Long.valueOf(t_timestamp.get(total_tweets-1));
					
					
				t_0 = Long.valueOf(t_timestamp.get(limInf));
				t_N_2=Long.valueOf(t_timestamp.get(limMed));
		
				long restaA =  (t_N - t_0);
				long restaB =  (t_N - t_N_2);
				
				volume_momentum = ((double)(restaA - restaB) / restaA);
				
				
				//Aplicamos la fórmula para Popularity Momentum num_retweets_(N/2, N) / totalRetweets_(0,N)
				for(int i=total_tweets;i>limInf;i--)
					totalRetweets = totalRetweets+t_retweets.get(i-1);
				
				
				for(int i=total_tweets;i>limMed;i--)
					num_retweets = num_retweets+t_retweets.get(i-1);
					
				
				if(totalRetweets>0)//para evitar la división entre 0, en aquellos tweets que no hayan tenido nunca un retweet
					popularity_momentum =num_retweets / totalRetweets;
				
				
			}
			
			
			outputValue.setVolume_momentum(volume_momentum);
			outputValue.setPopularity_momentum(popularity_momentum);
			outputValue.setTotal_tweets(total_tweets);
			
			
			context.write(key, outputValue);
			
		}
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = super.getConf();
		
		//Obtenemos la configuración del sistema
		conf.addResource(new Path("file:///opt/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("file:///opt/hadoop/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path("file:///opt/hadoop/etc/hadoop/yarn-site.xml"));
		
		Job job = Job.getInstance(conf, "TwitterPreprocessor");
		job.setJarByClass(TweetPreprocessor.class);
							
		Path inDir = new Path("/user/hadoop/inTwitter");
		Path outDir = new Path("/user/hadoop/outTwitter");
				
		FileInputFormat.setInputPaths(job, inDir);
		FileOutputFormat.setOutputPath(job, outDir);
		outDir.getFileSystem(conf).delete(outDir, true);

				
		job.setMapperClass(PreprocessorMapper.class);
		job.setReducerClass(TweetReducer.class);
		
		job.setInputFormatClass(TweetInputFormat.class);		 
		job.setOutputFormatClass(TweetOutputFormat.class);
		
		job.setOutputKeyClass(Tweet.class);
		job.setOutputValueClass(TweetValues.class);
		
		job.setMapOutputKeyClass(Tweet.class);
		job.setMapOutputValueClass(TweetValues.class);

		job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(), new TweetPreprocessor(), args);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);
	}

}
