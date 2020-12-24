package MaxMin;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxMinTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int maxTemperature = Integer.MIN_VALUE;
		int minTemperature = Integer.MAX_VALUE;
		for (IntWritable value : values) {
			maxTemperature = Math.max(maxTemperature, value.get());
			minTemperature = Math.min(minTemperature, value.get());
		}
		System.out.println(key);
		System.out.println("\t Temp maxi = "+maxTemperature);
		System.out.println("\t Temp mini = "+minTemperature);
		System.out.println("\t.........");
		
		context.write(new Text(key+"\t Temp maxi = "), new IntWritable(maxTemperature));		
		context.write(new Text("\t Temp mini = "), new IntWritable(minTemperature));
		
	}
}


