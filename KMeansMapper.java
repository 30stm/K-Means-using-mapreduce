package mapreduce.MapreduceKMeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;

import Tools.ClusterCenter;
import Tools.Euclidean;
import Tools.Vector;

// first iteration, k-random centers, in every follow-up iteration we have new calculated centers
public class KMeansMapper extends
                Mapper<ClusterCenter, Vector, ClusterCenter, Vector> {

        List<ClusterCenter> centers = new ArrayList<ClusterCenter>();
//private int dimension;
        @SuppressWarnings("deprecation")
		@Override
        protected void setup(Context context) throws IOException,
                        InterruptedException {
                super.setup(context);
                Configuration conf = context.getConfiguration();
                
                Path centroids = new Path(conf.get("centroid.path"));
 //   			this.dimension = (conf.getInt("dimension",2));
    			
                FileSystem fs = FileSystem.get(conf);

                SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids,conf);
                ClusterCenter key = new ClusterCenter();
                IntWritable value = new IntWritable();
                while (reader.next(key, value)) {
                	centers.add(new ClusterCenter(key));
                }
                reader.close();
        }

        @Override
        protected void map(ClusterCenter key, Vector value, Context context)
                        throws IOException, InterruptedException {
        	
        		System.out.println("Centersize:"+centers.size());
        		
                ClusterCenter nearest = null;
                double nearestDistance = Double.MAX_VALUE;
                for (ClusterCenter c : centers) {
                        double dist = Euclidean.E_Distance(c, value);
                        if (nearest == null) {
                                nearest = c;
                                nearestDistance = dist;
                        } else {
                                if (nearestDistance > dist) {
                                        nearest = c;
                                        nearestDistance = dist;
                                }
                        }
                }
//                System.out.println("nearest:"+nearest);
//                System.out.println("value:"+value);
                context.write(nearest, value);
        }
}
