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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class PDPreProcess {

    public static class PreProcessMapper extends Mapper<Object, Text, LongWritable, MapWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split("\\s+");

            LongWritable nid = new LongWritable(Long.parseLong(str[0]));
            MapWritable adj = new MapWritable();
            adj.put(new LongWritable(Long.parseLong(str[1])), new LongWritable(Long.parseLong(str[2])));
            context.write(nid, adj);
            
            LongWritable adjNodeID = new LongWritable(Long.parseLong(str[1]));
            MapWritable emptyMap = new MapWritable();
            context.write(adjNodeID, emptyMap);
        }
    }

    public static class PreProcessReducer extends Reducer<LongWritable, MapWritable, LongWritable, PDNodeWritable> {

        public void reduce(LongWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            
            MapWritable adjlist = new MapWritable();
            for (MapWritable val: values) {
                adjlist.putAll(val);
            }

            PDNodeWritable node = new PDNodeWritable();
            node.setID(key);
            node.setDist(new LongWritable(Long.MAX_VALUE));
            node.setPred(new LongWritable(-1));
            node.setAdjlist(adjlist);
            node.setNode(true);

            if (ParallelDijkstra.debug){
            System.out.println("Key: " + node.getID());
            System.out.println("Dist: " + node.getDist());
            String l = new String();
            for(Map.Entry m: node.getAdjlist().entrySet()){
                l = l + "("+ m.getKey() + ", " + m.getValue()+ ") ";
            }
            System.out.println("AdjList: " + l);
            }
           

            context.write(key, node);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Running PDPreProcess");
        Configuration conf = new Configuration();
        // conf.set("fs.defaultFS", "file:///");
        // conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf, "PD PreProcess");
        job.setJarByClass(PDPreProcess.class);
        job.setMapperClass(PreProcessMapper.class);
        job.setReducerClass(PreProcessReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(PDNodeWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(ParallelDijkstra.tempPath, "PreProcess"));
        FileSystem.get(job.getConfiguration()).delete(new Path(ParallelDijkstra.tempPath, "PreProcess"), true);
        job.waitForCompletion(true);
    }
}
