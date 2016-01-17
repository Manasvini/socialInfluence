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
public static class EdgeWeightMapper
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
  public static class EdgeWeightReducer
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
    job1.setMapperClass(EdgeWeightMapper.class);
    job1.setReducerClass(EdgeWeightReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(args[1]));
    FileOutputFormat.setOutputPath(job1, new Path(args[2]));
    System.exit(job1.waitForCompletion(true) ? 0 : 1);
 
   }
}
