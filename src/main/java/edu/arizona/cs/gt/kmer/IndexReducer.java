package edu.arizona.cs.gt.kmer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text value : values) {
			String[] keyValue = value.toString().split("\t");
			keyText.set(keyValue[0]);
			valueText.set(keyValue[1]);
			context.write(keyText, valueText);
		}
	}
}
