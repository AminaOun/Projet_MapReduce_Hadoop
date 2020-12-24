package MaxMin;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxMinTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String annee = line.substring(15, 19);
		int Temperature;
		String st  = line.substring(2, 10);
		
		if (line.charAt(87) == '+') { 
			Temperature = Integer.parseInt(line.substring(88, 92));
		} else {
			Temperature = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92, 93);
		if (Temperature != MISSING && quality.matches("[01459]")) {
			context.write(new Text(annee+"  station: "+st+"\n"), new IntWritable(Temperature/10));
		}
	}
}




