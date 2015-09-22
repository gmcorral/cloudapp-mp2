import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
    	
    	Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Link Count");
        
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapOutputKeyClass(IntWritable.class);
        jobA.setMapOutputValueClass(IntWritable.class);
        
        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);
        
        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(PopularityLeague.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Links Rank");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);

        jobB.setMapperClass(LinkRankMap.class);
        jobB.setReducerClass(LinkRankReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(PopularityLeague.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static Set<Integer> readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));
        Set<Integer> lineSet = new HashSet<Integer>();
        
        String line;
        while( (line = buffIn.readLine()) != null)
        	lineSet.add(Integer.parseInt(line));
        
        return lineSet;
    }
    
    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
    	
    	private Set<Integer> leaguePages;
    	
    	@Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            
            String leaguePath = conf.get("league");

            this.leaguePages = readHDFSFile(leaguePath, conf);
        }
    	
    	@Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	String line = value.toString();
	    	
	    	int keySep = line.indexOf(":");
	    	if(keySep > -1) {
	        	try {
	        		String keyStr = line.substring(0, keySep);
	        		Integer keyInt = Integer.parseInt(keyStr);
	        		
	        		if(this.leaguePages.contains(keyInt))
	        			context.write(new IntWritable(keyInt), new IntWritable(0));
	            	
	            	line = line.substring(keySep + 1);
	            	StringTokenizer tokenizer = new StringTokenizer(line, " ");
		            while (tokenizer.hasMoreTokens()) {
		                String nextToken = tokenizer.nextToken().trim();
		                try {
		                	Integer keyId = Integer.parseInt(nextToken);
		                	if(this.leaguePages.contains(keyId))
		                		context.write(new IntWritable(keyId), new IntWritable(1));
		                } catch(NumberFormatException ignore) {
		                	System.err.println("Error parsing value: " + ignore.toString());
		                }
		            }
		            
	            } catch(Exception ignore) {
	            	System.err.println("Error parsing key: " + ignore.toString());
	            }
	    	} else {
	    		System.err.println("Error parsing line " + line);
	    	}
    	}
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    	
    	@Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    	int sum = 0;
	    	
	        for (IntWritable val : values)
	            sum += val.get();
	        
	        context.write(key, new IntWritable(sum));
    	}
    }

    public static class LinkRankMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        
        private List<Pair<Integer, Integer>> linkList = new ArrayList<Pair<Integer, Integer>>();
        
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	Integer count = Integer.parseInt(value.toString());
            Integer pageId = Integer.parseInt(key.toString());

            linkList.add(new Pair<Integer, Integer>(count, pageId));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for (Pair<Integer, Integer> item : linkList) {
                Integer[] values = {item.second, item.first};
                IntArrayWritable val = new IntArrayWritable(values);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class LinkRankReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        
        private TreeSet<Pair<Integer, Integer>> linkMap = new TreeSet<Pair<Integer, Integer>>();
        
        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        	for (IntArrayWritable val: values) {
                IntWritable[] pair= (IntWritable[]) val.toArray();

                Integer pageId = pair[0].get();
                Integer count = pair[1].get();

                linkMap.add(new Pair<Integer, Integer>(count, pageId));
            }

        	int rank = -1;
        	int previous = 0;
            for (Pair<Integer, Integer> item: linkMap) {
            	if(item.first > previous || rank < 0)
                	rank++;
                context.write(new IntWritable(item.second), new IntWritable(rank));
                previous = item.first;
            }
        }
    }
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = (o == null ? 1 : (this.first).compareTo(o.first));
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}