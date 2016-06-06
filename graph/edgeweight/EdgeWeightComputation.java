package graph.edgeweight;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
public class EdgeWeightComputation {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        word.set(value.toString().trim());
        context.write(word, one);
      }
    
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
public static class EdgeCountMapper
       extends Mapper<Object, Text, Text,Text>{


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String [] splits = value.toString().trim().split(",");
        String src = splits[0];
	String dest = splits[1].split("\t")[0];
  	String cost = splits[1].split("\t")[1]; 
        context.write(new Text(src),new Text(dest+","+cost+";" ));
      }

  }
  public static class EdgeCountReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String edges = ""; 
      for (Text val : values) {
       edges += val.toString(); 
      }
      result.set(edges);
      context.write(key, result);
    }
  }

  public static class MaxEdgeCountMapper
       extends Mapper<Object, Text, Text,IntWritable>{


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String [] splits = value.toString().trim().split("\t");
        String src = splits[0].trim();
        String [] dests = splits[1].split(";");
	for(String s : dests){
		String dest = s.trim().split(",")[0];
		int count = Integer.parseInt(s.split(",")[1]);
		if( dest.equals(src)){
			context.write(new Text("maxna"),new IntWritable(count));	
		}
		else{
			context.write(new Text("maxia"), new IntWritable(count));
		}
	}
    }
  } 

  public static class MaxEdgeCountReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int max = 0;
      for (IntWritable val : values) {
           if(val.get() > max)
		max = val.get();	
      }
      context.write(key, new IntWritable(max));
    }
  }

  public static class EdgeWeightMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String [] splits = value.toString().trim().split("\t");
        String src = splits[0].trim();
        String [] dests = splits[1].split(";");
	int na = 0;
	int sum_ia = 0;
	double alpha = 0.5;
	double max_na = 1;
        for(String s : dests){
                String dest = s.trim().split(",")[0];
                int count = Integer.parseInt(s.split(",")[1]);
                if( dest.equals(src)){
                	na = count;
		}
                else{
                	sum_ia += count;
		}
        }
	for(String s: dests){
		String dest = s.trim().split(",")[0];
                int count = Integer.parseInt(s.split(",")[1]);
		double w_na = 0;
		if ( na > 0)
			w_na = alpha * (na / max_na);
		double weight = w_na  + ( 1 - alpha) *((double) count /(double)sum_ia);
		context.write(new Text(src + "," + dest), new DoubleWritable(weight));
	}
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Compute Edge Weights");
    job.setJarByClass(EdgeWeightComputation.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);

    Job job1 = Job.getInstance(conf, "Aggregate edges");
    job1.setJarByClass(EdgeWeightComputation.class);
    job1.setMapperClass(EdgeCountMapper.class);
    job1.setReducerClass(EdgeCountReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(args[1]));
    FileOutputFormat.setOutputPath(job1, new Path(args[2]));
    job1.waitForCompletion(true); 
    
    Job job2 = Job.getInstance(conf, "compute maxna and maxia");
    job2.setJarByClass(EdgeWeightComputation.class);
    job2.setMapperClass(MaxEdgeCountMapper.class);
    job2.setReducerClass(MaxEdgeCountReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path(args[2]));
    FileOutputFormat.setOutputPath(job2, new Path(args[3]));
    job2.waitForCompletion(true);
   
    Job job3 = Job.getInstance(conf, "compute edge weights");
    job3.setJarByClass(EdgeWeightComputation.class);
    job3.setMapperClass(EdgeWeightMapper.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job3, new Path(args[2]));
    FileOutputFormat.setOutputPath(job3, new Path(args[4]));
    System.exit(job3.waitForCompletion(true) ? 0 : 1);

   }
}
