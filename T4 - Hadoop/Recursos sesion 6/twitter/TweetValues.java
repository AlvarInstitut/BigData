package ejercicio2.Twitter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/*
 * @author Carlos López Moreno
 */
public class TweetValues implements Writable {
	String created_at;
	int retweet_count;
	int total_tweets;
	double volume_momentum;
	double popularity_momentum;
	
	private static final String DELIMITER = "\t";
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(created_at);
		out.writeInt(retweet_count);
		out.writeInt(total_tweets);
		out.writeDouble(volume_momentum);
		out.writeDouble(popularity_momentum);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		created_at = in.readUTF();
		retweet_count = in.readInt();
		total_tweets = in.readInt();
		volume_momentum = in.readDouble();
		popularity_momentum = in.readDouble();
	}
	
	public void setCreated_at(String created_at){
		this.created_at = created_at; 
	}
	
	public void setRetweet_count(int retweet_at){
		this.retweet_count=retweet_at;
	}
	
	public void setTotal_tweets(int total_tweets){
		this.total_tweets = total_tweets;
	}
	
	public void setVolume_momentum(double volume_momentum){
		this.volume_momentum=volume_momentum;
	}
	
	public void setPopularity_momentum(double popularity_momentum){
		this.popularity_momentum=popularity_momentum;
	}
	
	public double getVolume_momentum(){
		return this.volume_momentum;
	}
	
	public double getPopularity_momentum(){
		return popularity_momentum;
	}
	
	public String getCreated_at(){
		return this.created_at;
	}
	
	public int getRetweet_count(){
		return this.retweet_count;
	}
	
	public int getTotal_tweets(){
		return this.total_tweets;
	}

	public String toString() {
		return created_at + DELIMITER;//+ retweet_count;
	}
}
