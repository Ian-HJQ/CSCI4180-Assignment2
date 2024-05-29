import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;

public class PageRank {
    public static final String tempPath = new String("/user/hadoop/tmp_out");

    public static class FirstMapper extends Mapper<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        public void map(IntWritable key, PRNodeWritable value, Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            long nodeCount = Long.parseLong(conf.get("nodeCount"));

            PRNodeWritable node = new PRNodeWritable();

            double rank = (double)1/nodeCount;
            value.setRank(rank);

            context.write(key, value);

        }
    }

    public static class PRMapper extends Mapper<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable>{

        public void map(IntWritable key, PRNodeWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
            String[] adjlist = value.getAdjlist().toStrings();
            double r = (double)value.getRank().get()/adjlist.length;
            
            for (int i=0; i<adjlist.length; i++){
                PRNodeWritable node = new PRNodeWritable();
                int nid = Integer.parseInt(adjlist[i]);
                node.setID(nid);
                node.setRank(r);
                node.setAdjlist(value.getAdjlist());
                node.setNode(false);
                
                context.write(node.getID(), node);
            }
        }
    }

    public static class PRReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {

        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {
            PRNodeWritable node = new PRNodeWritable();
            double ranksum = 0.0;

            for(PRNodeWritable n: values){
                if (n.isNode()){
                    node.setID(n.getID().get());
                    node.setAdjlist(n.getAdjlist().get());
                    node.setNode(true);
                }
                else{
                    ranksum += n.getRank().get();
                }
            }
            node.setRank(ranksum);
            context.write(key, node);
        }
    }

    public static class TextOutputReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, DoubleWritable> {

        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            double threshold = Double.parseDouble(conf.get("threshold"));
            double ranksum = 0.0;

            for(PRNodeWritable n: values){

                if (n.isNode() == false){
                    ranksum += n.getRank().get();
                }
            }

            if (ranksum > threshold){
                DoubleWritable rank = new DoubleWritable(ranksum);
                context.write(key, rank);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4){
            System.out.println("Missing Argument(s)");
            System.exit(1);
        }
        int max_iters = Integer.parseInt(args[0]);
        int count = 0;

        if (max_iters == 0) {
            System.exit(0);
        }

        Configuration conf = new Configuration();
        // conf.set("fs.defaultFS", "file:///");
        // conf.set("mapreduce.framework.name", "local");
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        conf.set("threshold", args[1]);
        conf.set("max_iters", args[0]);
        
        Job PreProcessJob = Job.getInstance(conf, "PR PreProcess");
        PRPreProcess.main(args, PreProcessJob);

        Job FirstIter = Job.getInstance(conf, "First Iteration");
        Counter nodeCount = PreProcessJob.getCounters().findCounter(PRPreProcess.NodeCounters.nodeCount);

        if (max_iters == 1){
            System.out.println("Running First Iter...");
            FirstIter.getConfiguration().setLong("nodeCount", nodeCount.getValue());
            FirstIter.setJarByClass(PageRank.class);
            FirstIter.setMapperClass(FirstMapper.class);
            FirstIter.setReducerClass(TextOutputReducer.class);
            FirstIter.setMapOutputKeyClass(IntWritable.class);
            FirstIter.setMapOutputValueClass(PRNodeWritable.class);
            FirstIter.setOutputKeyClass(IntWritable.class);
            FirstIter.setOutputValueClass(DoubleWritable.class);
            FirstIter.setInputFormatClass(SequenceFileInputFormat.class);
            FileInputFormat.addInputPath(FirstIter, new Path(tempPath, "PreProcess"));
            FileOutputFormat.setOutputPath(FirstIter, new Path(args[3]));
            FileSystem.get(FirstIter.getConfiguration()).delete(new Path(args[3]), true);
            FirstIter.waitForCompletion(true);
            FileSystem.get(FirstIter.getConfiguration()).delete(new Path(tempPath), true);
        }
        else {
            System.out.println("Running First Iter...");
            FirstIter.getConfiguration().setLong("nodeCount", nodeCount.getValue());
            FirstIter.setJarByClass(PageRank.class);
            FirstIter.setMapperClass(FirstMapper.class);
            FirstIter.setOutputKeyClass(IntWritable.class);
            FirstIter.setOutputValueClass(PRNodeWritable.class);
            FirstIter.setNumReduceTasks(0);
            FirstIter.setInputFormatClass(SequenceFileInputFormat.class);
            FirstIter.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(FirstIter, new Path(tempPath, "PreProcess"));
            FileOutputFormat.setOutputPath(FirstIter, new Path(tempPath, "out" + count));
            FileSystem.get(FirstIter.getConfiguration()).delete(new Path(tempPath, "out" + count), true);
            FirstIter.waitForCompletion(true);
            FileSystem.get(FirstIter.getConfiguration()).delete(new Path(tempPath, "PreProcess"), true);
            count++;
        }
        
        
        while (count < max_iters-1){
            System.out.println("Running Iter" + (count+1));
            Job job = Job.getInstance(conf, "PageRank");

            job.setJarByClass(PageRank.class);
            job.setMapperClass(PRMapper.class);
            job.setReducerClass(PRReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(PRNodeWritable.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(tempPath, "out"+(count-1)));
            FileOutputFormat.setOutputPath(job, new Path(tempPath, "out"+count));
            FileSystem.get(job.getConfiguration()).delete(new Path(tempPath, "out"+count), true);

            job.waitForCompletion(true);

            if (count == 1){
                FileSystem.get(job.getConfiguration()).delete(new Path(tempPath, "PreProcess"), true);
            }
            else if (count > 1){
                FileSystem.get(job.getConfiguration()).delete(new Path(tempPath, "out"+(count-1)), true);
            }
            count++;
        }

        if (max_iters > 1){
            System.out.println("Running Last Iter...");
            Job job1 = Job.getInstance(conf, "PageRank");

            job1.setJarByClass(PageRank.class);
            job1.setMapperClass(PRMapper.class);
            job1.setReducerClass(TextOutputReducer.class);
            job1.setMapOutputKeyClass(IntWritable.class);
            job1.setMapOutputValueClass(PRNodeWritable.class);
            job1.setOutputKeyClass(IntWritable.class);
            job1.setOutputValueClass(DoubleWritable.class);
            job1.setInputFormatClass(SequenceFileInputFormat.class);

            FileInputFormat.addInputPath(job1, count == 0 ? new Path(tempPath, "out"+(count-1)) : new Path(tempPath, "out"+(count-1)));
            FileOutputFormat.setOutputPath(job1, new Path(args[3]));
            FileSystem.get(job1.getConfiguration()).delete(new Path(args[3]), true);
            job1.waitForCompletion(true);
            FileSystem.get(job1.getConfiguration()).delete(new Path(tempPath), true);
        }
        System.exit(0);
    }
}