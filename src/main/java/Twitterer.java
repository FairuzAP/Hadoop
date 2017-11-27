import java.io.IOException;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Set;
import java.util.PriorityQueue;
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
    
    public static final int topRetrieved = 10;
    public static class Tuple<X, Y> { 
	public final X x; 
	public final Y y; 
	public Tuple(X x, Y y) { 
	    this.x = x; 
	    this.y = y; 
	} 
    }
    
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
	
	private String lineHolder;
	private String[] tokenHolder;
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    StringTokenizer lines = new StringTokenizer(value.toString(), "\n");
	    while (lines.hasMoreTokens()) {
		lineHolder = lines.nextToken();
		tokenHolder = lineHolder.split("\\s+");

		if(tokenHolder.length == 2) {
		    userID.set(tokenHolder[0]);
		    followerID.set(tokenHolder[1]);
		    
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
	private Text[] arrayHolder;
	private final TextArrayWritable followerIDArrWritable = new TextArrayWritable(new Text[0]);
	
	@Override
	public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
	    followerIDSet.clear();
	    userIDSet.clear();
	    
	    for(TextArrayWritable arr : values) {
		arrayHolder = arr.get();
		userIDSet.add(arrayHolder[0]);
		if (arrayHolder.length == 2) {
		    followerIDSet.add(arrayHolder[1]);
		}
	    }
	    
	    if(!followerIDSet.isEmpty()) {
		arrayHolder = followerIDSet.toArray(new Text[0]);
		followerIDArrWritable.set(arrayHolder);
		for(Text user : userIDSet) {
		    context.write(user, followerIDArrWritable);
		}
	    }
	}
    }

    /**
     * Map each records {<'A',['B','C']>, <'A',['D']>} to
     *			{<'A',['B','C','D']>}
     */
    public static class AggregatorReducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {
	Set<Text> followerIDSet = new HashSet<>();
	private final TextArrayWritable followerIDArrWritable = new TextArrayWritable(new Text[0]);
	private Text[] arrayHolder;
	
	@Override
	public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
	    followerIDSet.clear();
	    
	    for(TextArrayWritable arr : values) {
		arrayHolder = arr.get();
		followerIDSet.addAll(Arrays.asList(arrayHolder));
	    }
	    
	    if(!followerIDSet.isEmpty()) {
		arrayHolder = followerIDSet.toArray(new Text[0]);
		followerIDArrWritable.set(arrayHolder);
		context.write(key, followerIDArrWritable);
	    }
	}
    }
    
    public static class CounterMapper extends Mapper<Text, TextArrayWritable, Text, IntWritable> {
	private final Text userID = new Text();
	private final IntWritable followerCount = new IntWritable();
	private final PriorityQueue<Tuple<String, Integer>> topUser = new PriorityQueue<>((Tuple<String, Integer> o1, Tuple<String, Integer> o2) -> o1.y.compareTo(o2.y));	
	private Text[] arrayHolder;
	
	@Override
	public void map(Text key, TextArrayWritable value, Context context) throws IOException, InterruptedException {
	    arrayHolder = value.get();
	    topUser.add(new Tuple<>(key.toString(), arrayHolder.length));
	    if(topUser.size() > topRetrieved) {
		topUser.remove();
	    }
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
	    while(topUser.size() > 0) {
		Tuple<String, Integer> val = topUser.remove();
		followerCount.set(val.y);
		userID.set(val.x);
		context.write(userID, followerCount);
	    }
	}
    }
    
    public static class Top10Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private final Text userID = new Text();
	private final IntWritable followerCount = new IntWritable();
	private final PriorityQueue<Tuple<String, Integer>> topUser = new PriorityQueue<>((Tuple<String, Integer> o1, Tuple<String, Integer> o2) -> o1.y.compareTo(o2.y));	
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    topUser.add(new Tuple<>(key.toString(), values.iterator().next().get()));
	    if(topUser.size() > topRetrieved) {
		topUser.remove();
	    }
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
	    while(topUser.size() > 0) {
		Tuple<String, Integer> val = topUser.remove();
		followerCount.set(val.y);
		userID.set(val.x);
		context.write(userID, followerCount);
	    }
	}
    }
    
    public static void main(String[] args) throws Exception {
	
	// Job 1, TokennizerMapper -> FollowerReducer
	Configuration conf = new Configuration(); conf.set("mapreduce.map.java.opts", "-Xmx200m"); conf.set("mapreduce.reduce.java.opts", "-Xmx200m");
	FileSystem hdfs = FileSystem.get(conf);
	Job job1 = Job.getInstance(conf, "job_1_13514104");
	job1.setJarByClass(Twitterer.class); job1.setMapperClass(TokenizerMapper.class); job1.setReducerClass(FollowerReducer.class);
	job1.setNumReduceTasks(16);
	job1.setOutputFormatClass(SequenceFileOutputFormat.class); job1.setOutputKeyClass(Text.class); job1.setOutputValueClass(TextArrayWritable.class);
	
	Path in = new Path(args[0]); Path out = new Path(args[1] + "/1");
	if(hdfs.exists(out)) hdfs.delete(out, true);
	FileInputFormat.addInputPath(job1, in); FileOutputFormat.setOutputPath(job1, out);
	job1.waitForCompletion(true);
	
	// Jon 2, IdentityMapper -> Top10Reducer
	conf = new Configuration();  conf.set("mapreduce.map.java.opts", "-Xmx200m"); conf.set("mapreduce.reduce.java.opts", "-Xmx200m");
	Job job2 = Job.getInstance(conf, "job_2_13514104"); 
	job2.setJarByClass(Twitterer.class); job2.setCombinerClass(AggregatorReducer.class); job2.setReducerClass(AggregatorReducer.class);
	job2.setNumReduceTasks(16);
	job2.setInputFormatClass(SequenceFileInputFormat.class);
	job2.setOutputFormatClass(SequenceFileOutputFormat.class); job2.setOutputKeyClass(Text.class); job2.setOutputValueClass(TextArrayWritable.class);
	
	in = new Path(args[1] + "/1"); out = new Path(args[1] + "/2");
	if(hdfs.exists(out)) hdfs.delete(out, true);
	FileInputFormat.addInputPath(job2, in); FileOutputFormat.setOutputPath(job2, out);
	job2.waitForCompletion(true);
	
	if(hdfs.exists(in)) hdfs.delete(in, true);
	
	// Job 3, CounterMapper -> Top10Reducer
	conf = new Configuration();  conf.set("mapreduce.map.java.opts", "-Xmx200m"); conf.set("mapreduce.reduce.java.opts", "-Xmx200m");
	Job job3 = Job.getInstance(conf, "job_3_13514104");
	job3.setJarByClass(Twitterer.class); job3.setMapperClass(CounterMapper.class); job3.setReducerClass(Top10Reducer.class);
	job3.setNumReduceTasks(1);
	job3.setInputFormatClass(SequenceFileInputFormat.class);
	job3.setOutputKeyClass(Text.class); job3.setOutputValueClass(IntWritable.class);
	
	in = new Path(args[1] + "/2"); out = new Path(args[1] + "/3");
	if(hdfs.exists(out)) hdfs.delete(out, true);
	FileInputFormat.addInputPath(job3, in); FileOutputFormat.setOutputPath(job3, out);
	job3.waitForCompletion(true);
	
	if(hdfs.exists(in)) hdfs.delete(in, true);
    }
}

