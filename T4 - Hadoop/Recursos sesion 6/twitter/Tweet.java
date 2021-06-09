package ejercicio2.Twitter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/*
 * @author Carlos López Moreno
 */

public class Tweet implements WritableComparable<Tweet> {
	private String screen_name;
	private long time_stamp;

	@Override
	public void readFields(DataInput in) throws IOException {
		screen_name = in.readUTF();
		time_stamp = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(screen_name);
		out.writeLong(time_stamp);
	}


	public String getSreen_name() {
		return screen_name;
	}

	public long getTime_stamp(){
		return time_stamp;
	}
	
	public void setScreen_name(String screen_name) {
		this.screen_name = screen_name;
	}

	public void setTime_stamp(long time_stamp){
		this.time_stamp = time_stamp;
	}
	
	@Override
	public int compareTo(Tweet arg0) {
		int response = screen_name.compareTo(arg0.screen_name);
		return (int) ( response != 0 ? response : (this.time_stamp - arg0.time_stamp));
	}

	public String toString() {
		return screen_name;
	}

}
