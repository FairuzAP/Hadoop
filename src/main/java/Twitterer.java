import java.io.IOException;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Twitterer {
    
    private static class TextArrayWritable extends ArrayWritable {

	public TextArrayWritable(Text[] values) {
	    super(Text.class, values);
	}

	@Override
	public Text[] get() {
	    return (Text[]) super.get();
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
    private static class TokenizerMapper extends Mapper<Object, Text, Text, TextArrayWritable>{
	
	private final Text user = new Text();
	private final Text follow = new Text();
	
	private final Text follower[] = new Text[1];
	private final Text followerArr[] = new Text[2];
	private final TextArrayWritable followers = new TextArrayWritable(new Text[0]);

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    StringTokenizer lines = new StringTokenizer(value.toString(), "\n");
	    while (lines.hasMoreTokens()) {
		String line = lines.nextToken();
		String[] tokens = line.split("\\s+");

		user.set(tokens[0]);
		follow.set(tokens[1]);
		
		followerArr[0] = user;
		followerArr[1] = follow;
		follower[0] = user;
		
		followers.set(followerArr);
		context.write(user, followers);
		
		user.set(tokens[1]);
		followers.set(follower);
		context.write(user, followers);
	    }
	}
    }

    /**
     * Map each records {<'A',['A','B']>, <'A',['A','D']>, <'A',['C']>} to
     *			{<'A',['B','D']>, <'C',['B','D']>}
     */
    private static class FollowerReducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {
	Set<Text> followerSet = new HashSet<>();
	Set<Text> followedSet = new HashSet<>();
	private final TextArrayWritable followers = new TextArrayWritable(new Text[0]);
	
	@Override
	public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
	    followerSet.clear();
	    followedSet.clear();
	    
	    for(TextArrayWritable arr : values) {
		Text[] follower = arr.get();
		followedSet.add(follower[0]);
		for(int i=1; i<follower.length; i++) {
		    followerSet.add(follower[i]);
		}
	    }
	    
	    Text[] followerArr = followerSet.toArray(new Text[0]);
	    followers.set(followerArr);
	    for(Text user : followerSet) {
		context.write(user, followers);
	    }
	}
    }

    public static void main(String[] args) throws Exception {
	// Job 1
	Configuration conf = new Configuration();
	Job job1 = Job.getInstance(conf, "job_1_13514104");
	job1.setJarByClass(Twitterer.class);
	job1.setMapperClass(TokenizerMapper.class);
	job1.setReducerClass(FollowerReducer.class);
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(TextArrayWritable.class);
	
	Path in = new Path(args[0]);
	Path out = new Path(args[1] + "/1");
	FileInputFormat.addInputPath(job1, in);
	FileOutputFormat.setOutputPath(job1, out);
	job1.waitForCompletion(true);
    }
}

