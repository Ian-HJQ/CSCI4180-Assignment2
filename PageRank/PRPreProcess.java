import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;

public class PRPreProcess {
   
    public static enum NodeCounters {nodeCount};

    public static class PreProcessMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String [] str = value.toString().split("\\s+");

            IntWritable nid = new IntWritable(Integer.parseInt(str[0]));
            IntWritable adj = new IntWritable(Integer.parseInt(str[1]));
            System.out.println("nid: " + nid + ", adj: " + adj);
            context.write(nid, adj);
        }
    }

    public static class PreProcessReducer extends Reducer<IntWritable, IntWritable, IntWritable, PRNodeWritable> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<IntWritable> arrlist = new ArrayList<IntWritable>();
            System.out.println("Key: " + key);

            for(IntWritable v:values){
                IntWritable adj = new IntWritable(v.get());
                arrlist.add(adj);
            }
            
            IntArrayWritable adjlist = new IntArrayWritable();
            IntWritable[] arr = new IntWritable[arrlist.size()];
            arr = arrlist.toArray(arr);
            adjlist.set(arr);

            PRNodeWritable node = new PRNodeWritable();
            node.setID(key);
            node.setAdjlist(adjlist);
            node.setNode(true);
            IntWritable[] e = (IntWritable[])node.getAdjlist().get();
            for (int j=0; j<e.length ; j++){
                System.out.print(e[j] + " ");
            }
            System.out.println(" ");
            context.write(key, node);
            context.getCounter(NodeCounters.nodeCount).increment(1);
            System.out.println("isnode: "+ node.isNode());
        }
    }

    public static void main(String[] args, Job job) throws Exception {
        System.out.println("Running PRPreProcess");
        // Configuration conf = new Configuration();
        // conf.set("fs.defaultFS", "file:///");
        // conf.set("mapreduce.framework.name", "local");
        // Job job = Job.getInstance(conf, "PR PreProcess");
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PreProcessMapper.class);
        job.setReducerClass(PreProcessReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(PageRank.tempPath, "PreProcess"));
        FileSystem.get(job.getConfiguration()).delete(new Path(PageRank.tempPath, "PreProcess"), true);
        job.waitForCompletion(true);
    }
}