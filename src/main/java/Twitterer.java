import java.io.IOException;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class Twitterer {
    
    public static class TextArrayWritable extends ArrayWritable {
	public TextArrayWritable(Text[] values) {
	    super(Text.class, values);
	}
	public TextArrayWritable() {
	    super(Text.class);
	}
	
	@Override
	public Text[] get() {
	    Writable[] values = super.get();
	    Text[] res = new Text[values.length];
	    for(int i=0; i<values.length; i++) {
		Text text = (Text)values[i];  // cast
		res[i] = text;
	    }
	    return res;
	}
	@Override
	public String toString() {
	    Text[] values = get();
	    return Arrays.toString(values);
	}
    }
    
    /**
     * Map each record "'A' \t 'B' \n" into <'A',['A','B']> and <'B','A'>
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, TextArrayWritable>{
	
	private final Text userID = new Text();
	private final Text followerID = new Text();
	
	private final Text[] followerIDArr1 = new Text[1];
	private final Text[] followerIDArr2 = new Text[2];
	private final TextArrayWritable followerIDArrWritable = new TextArrayWritable(new Text[0]);

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    StringTokenizer lines = new StringTokenizer(value.toString(), "\n");
	    while (lines.hasMoreTokens()) {
		String line = lines.nextToken();
		String[] tokens = line.split("\\s+");

		if(tokens.length == 2) {
		    userID.set(tokens[0]);
		    followerID.set(tokens[1]);
		    
		    followerIDArr1[0] = userID;
		    followerIDArr2[0] = userID;
		    followerIDArr2[1] = followerID;

		    followerIDArrWritable.set(followerIDArr2);
		    context.write(userID, followerIDArrWritable);
		    
		    followerIDArrWritable.set(followerIDArr1);
		    context.write(followerID, followerIDArrWritable);
		}
	    }
	}
    }

    /**
     * Map each records {<'A',['A','B']>, <'A',['A','D']>, <'A',['C']>} to
     *			{<'A',['B','D']>, <'C',['B','D']>}
     */
    public static class FollowerReducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {
	Set<Text> followerIDSet = new HashSet<>();
	Set<Text> userIDSet = new HashSet<>();
	private final TextArrayWritable followerIDArrWritable = new TextArrayWritable(new Text[0]);
	
	@Override
	public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
	    followerIDSet.clear();
	    userIDSet.clear();
	    
	    for(TextArrayWritable arr : values) {
		Text[] follower = arr.get();
		userIDSet.add(follower[0]);
		for(int i=1; i<follower.length; i++) {
		    followerIDSet.add(follower[i]);
		}
	    }
	    
	    if(!followerIDSet.isEmpty()) {
		Text[] followerArr = followerIDSet.toArray(new Text[0]);
		followerIDArrWritable.set(followerArr);
		for(Text user : userIDSet) {
		    context.write(user, followerIDArrWritable);
		}
	    }
	}
    }
    
    public static class AggregatorReducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {
	Set<Text> followerIDSet = new HashSet<>();
	private final TextArrayWritable followerIDArrWritable = new TextArrayWritable(new Text[0]);
	
	@Override
	public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
	    followerIDSet.clear();
	    
	    for(TextArrayWritable arr : values) {
		Text[] follower = arr.get();
		followerIDSet.addAll(Arrays.asList(follower));
	    }
	    
	    if(!followerIDSet.isEmpty()) {
		Text[] followerArr = followerIDSet.toArray(new Text[0]);
		followerIDArrWritable.set(followerArr);
		context.write(key, followerIDArrWritable);
	    }
	}
    }
    
    public static void main(String[] args) throws Exception {
	
	// Job 1, TokennizerMapper -> FollowerReducer
	Configuration conf = new Configuration();
	FileSystem hdfs = FileSystem.get(conf);
	Job job1 = Job.getInstance(conf, "job_1_13514104");
	job1.setJarByClass(Twitterer.class);
	job1.setMapperClass(TokenizerMapper.class);
	job1.setReducerClass(FollowerReducer.class);
	job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(TextArrayWritable.class);
	
	Path in = new Path(args[0] + "/0"); Path out = new Path(args[0] + "/1");
	if(hdfs.exists(out)) hdfs.delete(out, true);
	FileInputFormat.addInputPath(job1, in);
	FileOutputFormat.setOutputPath(job1, out);
	job1.waitForCompletion(true);
	
	// Jon 2, IdentityMapper -> AggregatorReducer
	conf = new Configuration();
	Job job2 = Job.getInstance(conf, "job_2_13514104");
	job2.setJarByClass(Twitterer.class);
	job2.setReducerClass(AggregatorReducer.class);
	job2.setInputFormatClass(SequenceFileInputFormat.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(TextArrayWritable.class);
	
	in = new Path(args[0] + "/1"); out = new Path(args[0] + "/2");
	if(hdfs.exists(out)) hdfs.delete(out, true);
	SequenceFileInputFormat.addInputPath(job2, in);
	FileOutputFormat.setOutputPath(job2, out);
	job2.waitForCompletion(true);
	
    }
}

