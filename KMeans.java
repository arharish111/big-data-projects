import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Point implements WritableComparable {
    public double x;
    public double y;

    Point(){}

    Point(double ptx,double pty){
        x = ptx;
        y = pty;
    }

    public int compareTo(Object o) {
        Point q = (Point) o;
        if(this.x == q.x)
        {
            if(this.y < q.y)
                return -1;
            else if(this.y > q.y)
                return 1;
            else return 0;
        }
        else if(this.x < q.x)
            return -1;
        else return 1;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(x);
        dataOutput.writeDouble(y);
    }

    public void readFields(DataInput dataInput) throws IOException {
        x = dataInput.readDouble();
        y = dataInput.readDouble();
    }

    public String toString () { return x+","+y; }
}

class Avg implements Writable{
    public double sumX;
    public double sumY;
    public double count;

    Avg(){}

    Avg(double sX,double sY,double c){
        sumX = sX;
        sumY = sY;
        count = c;
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeDouble(sumX);
        dataOutput.writeDouble(sumY);
        dataOutput.writeDouble(count);
    }

    public void readFields(DataInput dataInput) throws IOException {

        sumX = dataInput.readDouble();
        sumY = dataInput.readDouble();
        count = dataInput.readDouble();
    }
}

public class KMeans {

    static Vector<Point> centroids = new Vector<Point>(100);

    static HashMap<Point,Avg> table;

    public static class AvgMapper extends Mapper<Object,Text,Point,Avg> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] paths = context.getCacheFiles();
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
            String line;
            while ((line = reader.readLine()) != null){
                Scanner s = new Scanner(line).useDelimiter(",");
                Point p = new Point(s.nextDouble(),s.nextDouble());
                centroids.add(p);
            }
            table = new HashMap<Point, Avg>();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Point c:table.keySet()) {
                context.write(c,table.get(c));
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            Point p = new Point(s.nextDouble(),s.nextDouble());
            Point closest = new Point();
            double min = Double.MAX_VALUE;
            for (Point c:centroids){
                if(distance(c,p)<min){
                    closest = c;
                    min= distance(c,p);
                }
            }
            if(table.get(closest)==null){
                table.put(closest,new Avg(p.x,p.y,1));
            }
            else{
                table.put(closest,
                        new Avg(table.get(closest).sumX+p.x,table.get(closest).sumY+p.y,table.get(closest).count+1));
            }
            s.close();
        }

        private static double distance(Point c, Point pt) {
            double first = Math.pow((pt.x-c.x),2);
            double second =  Math.pow((pt.y-c.y),2);
            return Math.sqrt(first + second);
        }
    }

    public static class AvgReducer extends Reducer<Point,Avg,Point,Object> {

        @Override
        protected void reduce(Point key, Iterable<Avg> values, Context context) throws IOException, InterruptedException {

            double count = 0;
            double sx = 0.0;
            double sy = 0.0;
            for (Avg a: values){
                count += a.count;
                sx += a.sumX;
                sy += a.sumY;
            }
            key.x = sx/count;
            key.y = sy/count;
            context.write(key,null);
        }
    }

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("KMeans");
        job.setJarByClass(KMeans.class);
        job.setOutputKeyClass(Point.class);
        job.setOutputValueClass(Point.class);
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(Avg.class);
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.addCacheFile(new URI(args[1]));
        job.waitForCompletion(true);
   }
}