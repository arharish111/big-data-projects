import java.io.*;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth

    Vertex(){}

    Vertex(long i,Vector<Long> adj,long c,short d)
    {
        id = i;
        adjacent = adj;
        centroid = c;
        depth = d;
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeLong(id);
        dataOutput.writeInt(adjacent.size());
        for (int i=0;i<adjacent.size();i++) {
            dataOutput.writeLong(adjacent.get(i));
        }
        dataOutput.writeLong(centroid);
        dataOutput.writeShort(depth);
    }

    public void readFields(DataInput dataInput) throws IOException {

        id = dataInput.readLong();
        int l = dataInput.readInt();
        adjacent = new Vector<Long>();
        for(int i=0;i<l;i++){
            adjacent.add(dataInput.readLong());
        }
        centroid = dataInput.readLong();
        depth = dataInput.readShort();
    }

/*
    @Override
    public String toString() {
        return id+","+adjacent+","+centroid+","+depth;
    }
*/
}

public class GraphPartition {
    //static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;
    static long count = -1;

    public static class GraphMapper extends Mapper<Object,Text,LongWritable,Vertex>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            count += 1;
            Vector<Long> adjs = new Vector<Long>();
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long id = s.nextLong();
            while (s.hasNext()){
                adjs.add(s.nextLong());
            }
            if(count<10)
                context.write(new LongWritable(id),new Vertex(id,adjs,id, (short) 0));
            else
                context.write(new LongWritable(id),new Vertex(id,adjs,-1,(short) 0));
        }
    }

    public static class BFSMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex>{

        @Override
        protected void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(value.id),value);
            if(value.centroid > 0)
            {
                for (long n:value.adjacent){
                    context.write(new LongWritable(n),new Vertex(n,new Vector<Long>(),value.centroid,BFS_depth));
                }
            }
        }
    }

    public static class BFSReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex>{

        @Override
        protected void reduce(LongWritable key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
               short min_depth = 10000;
               Vertex m = new Vertex(key.get(),new Vector<Long>(),-1, (short) 0);
               for(Vertex v:values){
                    if(!v.adjacent.isEmpty()){
                        m.adjacent = v.adjacent;
                    }
                    if(v.centroid > 0 && v.depth < min_depth){
                        min_depth = v.depth;
                        m.centroid = v.centroid;
			m.depth = min_depth;
                    }
               }
              // m.depth = min_depth;
               context.write(key,new Vertex(m.id,m.adjacent,m.centroid,m.depth));
        }
    }

    public static class ClusterMapper extends Mapper<LongWritable,Vertex,LongWritable,LongWritable>{
        @Override
        protected void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(value.centroid),new LongWritable(1));
        }
    }

    public static class ClusterReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>{
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long m = 0;
            for(LongWritable v:values){
                m = m + v.get();
            }
            context.write(key,new LongWritable(m));
        }
    }

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        /* ... First Map-Reduce job to read the graph */
        job.setJobName("GraphReadJob");
        job.setJarByClass(GraphPartition.class);
        job.setMapperClass(GraphMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i0"));
        job.waitForCompletion(true);
        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            job = Job.getInstance();
            /* ... Second Map-Reduce job to do BFS */
            job.setJarByClass(GraphPartition.class);
            job.setJobName("BFSJob");
            job.setMapperClass(BFSMapper.class);
            job.setReducerClass(BFSReducer.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job,new Path(args[1]+"/i"+i));
            FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i"+(i+1)));
            job.waitForCompletion(true);
        }
        job = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the cluster sizes */
        job.setJarByClass(GraphPartition.class);
        job.setJobName("ClusterCountJob");
        job.setMapperClass(ClusterMapper.class);
        job.setReducerClass(ClusterReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[1]+"/i8"));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}
