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
public class DiffusionProbabilityComputation {

public static class EdgeProbabilityMapper
       extends Mapper<Object, Text, Text,IntWritable>{

  public static int getBinomial( double p) {
  	int x = 0;
  		  if(Math.random() < p)
  		    return 0;
  	return 1;
   }
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String [] splits = value.toString().trim().split("\t");
  	double prob = Double.parseDouble(splits[1].trim());
        context.write(new Text(splits[0]),new IntWritable(getBinomial(prob)));
      }

  }
/*  public static class EdgeCountReducer
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
*/

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Compute Edge Weights");
    job.setJarByClass(DiffusionProbabilityComputation.class);
    job.setMapperClass(EdgeProbabilityMapper.class);
   // job.setCombinerClass(IntSumReducer.class);
   // job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

   }
}
