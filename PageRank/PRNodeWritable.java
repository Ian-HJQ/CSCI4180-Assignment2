import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;

public class PRNodeWritable implements Writable {
    private IntWritable nid;
    private IntArrayWritable adjlist;
    private DoubleWritable rank;
    private BooleanWritable flag;

    public PRNodeWritable() {
        nid = new IntWritable();
        adjlist = new IntArrayWritable();
        rank = new DoubleWritable();
        flag = new BooleanWritable();
    }

    public void setID(IntWritable nid){
        this.nid = nid;
    }
    public void setID(int nid){
        this.nid.set(nid);
    }

    public void setAdjlist(IntArrayWritable adjlist){
        this.adjlist = adjlist;
    }
    public void setAdjlist(Writable[] adjlist){
        this.adjlist.set(adjlist);
    }

    public void setRank(DoubleWritable rank){
        this.rank = rank;
    }
    public void setRank(double rank){
        this.rank.set(rank);
    }

    public void setNode(BooleanWritable flag){
        this.flag = flag;
    }
    public void setNode(boolean flag){
        this.flag.set(flag);
    }

    public IntWritable getID(){
        return nid;
    }

    public IntArrayWritable getAdjlist(){
        return adjlist;
    }

    public DoubleWritable getRank() {
        return rank;
    }

    public boolean isNode(){
        return flag.get();
    }

    public void write(DataOutput out) throws IOException {
        nid.write(out);
        adjlist.write(out);
        rank.write(out);
        flag.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        nid.readFields(in);
        adjlist.readFields(in);
        rank.readFields(in);
        flag.readFields(in);
    }
}