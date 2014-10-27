package mapreduce.MapreduceKMeans;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import Tools.ClusterCenter;
import Tools.Vector;

public class CreateCentroids {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		Path center = new Path("MRKmeans/clustering/import/center/cen.seq");
		Configuration conf = new Configuration();
			
		Random rand = new Random();
		
        int dimension =Integer.parseInt(args[0]);
        int num_centers = Integer.parseInt(args[1]);

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(center))
            fs.delete(center, true);

        double rand_centers[] = new double[dimension];       
        final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center, ClusterCenter.class, IntWritable.class);
        final IntWritable value = new IntWritable(0);
        
        for(int i=0;i<num_centers;i++)
        {
        	for(int j=0;j<dimension;j++)
        	{
        		rand_centers[j] = rand.nextInt(1000);
        	}
           	centerWriter.append(new ClusterCenter(new Vector(rand_centers)), value);
        }
        centerWriter.close();
	}

}
