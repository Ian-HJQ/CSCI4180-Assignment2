import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class ParallelDijkstra {
    public static final String tempPath = new String("/user/hadoop/tmp_out");
    public static final boolean debug = false; //whether to print debug message

    public static final long inf = Long.MAX_VALUE;

    public static void printNode(PDNodeWritable node) { //for debug
        System.out.println("Node: " + node.getID());
        System.out.println("Dist: " + node.getDist());
        System.out.println("Pred: " + node.getPred());
        String l = new String();
        for(Map.Entry m: node.getAdjlist().entrySet()){
            l = l + "("+ m.getKey() + ", " + m.getValue()+ ") ";
        }
        System.out.println("AdjList: " + l);
        System.out.println("Flag: " + node.isNode());
    }

    public static class PDMapper extends Mapper<LongWritable, PDNodeWritable, LongWritable, PDNodeWritable>{

        public void map(LongWritable key, PDNodeWritable value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            LongWritable src = new LongWritable(Long.parseLong(conf.get("src")));

            if (debug){
            System.out.println("Mapper Input. Key: " + key);
            printNode(value);
            System.out.println(" ");
            }

            if(value.getID().equals(src)){
                value.setDist(0);
                value.setPred(src.get());
            }
            
            LongWritable d = value.getDist();

            if (debug){
            System.out.println("Mapper Output. Key: " + key);
            printNode(value);
            System.out.println(" ");
            }

            context.write(key, value);

            for(Map.Entry adjlist: value.getAdjlist().entrySet()){
                PDNodeWritable node = new PDNodeWritable();
                node.setID(((LongWritable)adjlist.getKey()).get());
                node.setDist(d.get()==inf ? inf : d.get() + ((LongWritable)adjlist.getValue()).get());
                node.setPred(key);
                node.setNode(false);

                LongWritable nid = new LongWritable(node.getID().get());

                if (debug){
                System.out.println("Mapper Output. Key: " + nid);
                printNode(node);
                System.out.println(" "); 
                }
                

                context.write(nid, node);
            }
        }
    }

    public static class PDReducer extends Reducer<LongWritable, PDNodeWritable, LongWritable, PDNodeWritable> {

        public void reduce(LongWritable key, Iterable<PDNodeWritable> values, Context context) throws IOException, InterruptedException {
            if (debug) System.out.println("Reducer Input. Key: " + key);

            long dMin = inf;
            long pred = -1;
            PDNodeWritable node = new PDNodeWritable();
            
            for(PDNodeWritable n: values){

                if (debug){
                printNode(n);
                System.out.println(" ");
                }
                

                if (n.isNode()){
                    node.setID(n.getID().get());
                    node.setDist(n.getDist().get());
                    node.setPred(n.getPred().get());
                    node.setAdjlist(new MapWritable(n.getAdjlist()));
                    node.setNode(n.isNode());
                }
                else if (n.getDist().get() < dMin) {
                    dMin = n.getDist().get();
                    pred = n.getPred().get();
                }
            }

            if (dMin < node.getDist().get()){
                node.setDist(dMin);
                node.setPred(pred);
            }

            if (debug) {
            System.out.println("Reducer Output. Key: " + key);
            printNode(node);
            System.out.println(" ");
            }
            
            context.write(key, node);
            
        }
    }

    public static class TextOutputReducer extends Reducer<LongWritable, PDNodeWritable, LongWritable, Text> {

        public void reduce(LongWritable key, Iterable<PDNodeWritable> values, Context context) throws IOException, InterruptedException {
            if (debug) System.out.println("Reducer Input. Key: " + key);

            long dMin = inf;
            long pred = -1;
            PDNodeWritable node = new PDNodeWritable();
            
            for(PDNodeWritable n: values){
                if (debug){
                printNode(n);
                System.out.println(" ");
                }

                if (n.isNode()){
                    node.setID(n.getID().get());
                    node.setDist(n.getDist().get());
                    node.setPred(n.getPred().get());
                    node.setAdjlist(new MapWritable(n.getAdjlist()));
                    node.setNode(n.isNode());
                }
                else if (n.getDist().get() < dMin) {
                    dMin = n.getDist().get();
                    pred = n.getPred().get();
                }
            }

            if (dMin < node.getDist().get()){
                node.setDist(dMin);
                node.setPred(pred);
            }

            if (debug){
            System.out.println("Reducer Output. Key: " + key);
            printNode(node);
            System.out.println(" ");
            }
            
            
            if (node.getDist().get() != inf){
                String out = new String();
                out = node.getDist().get() + " " + node.getPred().get();
                context.write(key, new Text(out));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4){
            System.out.println("Missing Argument(s)");
            System.exit(1);
        }
        PDPreProcess.main(args);

        long iters = Long.parseLong(args[3]);
        long count = 0;

        Configuration conf = new Configuration();
        // conf.set("fs.defaultFS", "file:///");
        // conf.set("mapreduce.framework.name", "local");
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        conf.set("src", args[2]);

        while (count < iters-1) {
            System.out.println("Running iter " + (count+1) + " of Parallel Dijkstra...");
            Job job = Job.getInstance(conf, "Parallel Dijkstra");

            job.setJarByClass(ParallelDijkstra.class);
            job.setMapperClass(PDMapper.class);
            job.setReducerClass(PDReducer.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(PDNodeWritable.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(job, count == 0 ? new Path(tempPath, "PreProcess") : new Path(tempPath, "out"+(count-1)));
            FileOutputFormat.setOutputPath(job, new Path(tempPath, "out"+count));
            FileSystem.get(job.getConfiguration()).delete(new Path(tempPath, "out"+count), true);

            if (job.waitForCompletion(true)) {
                count++;
            }

            if (count == 1){
                FileSystem.get(job.getConfiguration()).delete(new Path(tempPath, "PreProcess"), true);
            }
            else if (count >= 2){
                FileSystem.get(job.getConfiguration()).delete(new Path(tempPath, "out"+(count-2)), true);
            }
        }

        System.out.println("Running Last iter...");
        
        Job job1 = Job.getInstance(conf, "Parallel Dijkstra");

        job1.setJarByClass(ParallelDijkstra.class);
        job1.setMapperClass(PDMapper.class);
        job1.setReducerClass(TextOutputReducer.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(PDNodeWritable.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job1, count == 0 ? new Path(tempPath, "PreProcess") : new Path(tempPath, "out"+(count-1)));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        FileSystem.get(job1.getConfiguration()).delete(new Path(args[1]), true);
        job1.waitForCompletion(true);
        FileSystem.get(job1.getConfiguration()).delete(new Path(tempPath), true);
        System.exit(0);
    }
}