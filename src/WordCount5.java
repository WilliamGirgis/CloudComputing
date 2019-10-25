import java.io.BufferedReader;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.Normalizer;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.*	;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class WordCount5{
	
	public static class NameMapper extends Mapper <Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().toLowerCase();
			line = Normalizer.normalize(line, Normalizer.Form.NFD);
			line = line.replaceAll("[^\\p{ASCII}\\.]", "");
			String[] tokens = line.split("[^<>\\s']+");
			if(tokens[2].contentEquals("hasgivenname")) {
				context.write(new Text(tokens[1]), new Text(tokens[2] + " " + tokens[3]));
			}
		}
	}

	
	
	public static class LivesMapper extends Mapper<Object, Text, Text, Text> {
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().toLowerCase();
			line = Normalizer.normalize(line, Normalizer.Form.NFD);
			line = line.replaceAll("[^\\p{ASCII}\\.]", "");
			
			String[] tokens = line.split("[<>^\\s']+");
			if(tokens[2].contentEquals("livesin")) {
				context.write(new Text(tokens[1]), new Text(tokens[2] + " " + tokens[3]));
			}
		}
	}

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> valores, Context context)
				throws IOException, InterruptedException {
			String final_text = "";
			String given_name = "";
			String lives_in = "";
			
			for(Text txt: valores) {
				if(txt.toString().contains("hasgivenname")) {
					given_name = txt.toString();
				}
				if(txt.toString().contains("livesin")) {
					lives_in = txt.toString();
				}
			}
			final_text = given_name + lives_in;
			if(!given_name.contentEquals("") && !lives_in.contentEquals("")){
				context.write(key, new Text(final_text));
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "wordcount");

		//job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(JoinReducer.class); // enable to use 'local aggregation'
		job.setReducerClass(JoinReducer.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, NameMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, LivesMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}