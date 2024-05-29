import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.BooleanWritable;

public class PDNodeWritable implements Writable {
    private LongWritable nid;
    private LongWritable dist;
    private LongWritable pred;
    private MapWritable adjlist;
    private BooleanWritable flag;

    public PDNodeWritable() {
        nid = new LongWritable();
        dist = new LongWritable();
        pred = new LongWritable(); 
        adjlist = new MapWritable();
        flag = new BooleanWritable(); //true indicating this is a node struc,
                                      //false otherwise
    }

    public void setID(LongWritable nid){
        this.nid = nid;
    }

    public void setID(long nid){
        this.nid.set(nid);
    }

    public void setDist(LongWritable dist){
        this.dist = dist;
    }

    public void setDist(long dist){
        this.dist.set(dist);
    }

    public void setPred(LongWritable pred){
        this.pred = pred;
    }

    public void setPred(long pred){
        this.pred.set(pred);
    }

    public void setAdjlist(MapWritable adjlist){
        this.adjlist = adjlist;
    }

    public void setNode(BooleanWritable flag) {
        this.flag = flag;
    }

    public void setNode(boolean flag){
        this.flag.set(flag);
    }

    public LongWritable getID(){
        return nid;
    }

    public LongWritable getDist(){
        return dist;
    }

    public LongWritable getPred(){
        return pred;
    }

    public MapWritable getAdjlist(){
        return adjlist;
    }
    
    public boolean isNode(){
        return flag.get();
    }

    public void write(DataOutput out) throws IOException {
        nid.write(out);
        dist.write(out);
        pred.write(out);
        adjlist.write(out);
        flag.write(out);
    }

    public void readFields(DataInput in ) throws IOException {
        nid.readFields(in);
        dist.readFields(in);
        pred.readFields(in);
        adjlist.readFields(in);
        flag.readFields(in);
    }
}