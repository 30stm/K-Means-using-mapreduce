package Tools;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public class DatasetWriter {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		Random rand = new Random(); 
        Configuration conf = new Configuration();
        
        FileSystem fs = FileSystem.get(conf);
        
        
        int dimension =Integer.parseInt(args[0]);
        int num_data = Integer.parseInt(args[1]);
        Path in = new Path("MRKmeans/clustering/import/data");
        if (fs.exists(in))
            fs.delete(in, true);
        
        double rand_vectors[] = new double[dimension];
        double num_zero[] = new double[dimension];
        
        final SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf, in, ClusterCenter.class, Vector.class);
        for(int i=0;i<num_data;i++)
        {
        	for(int j=0;j<dimension;j++)
        	{
        		rand_vectors[j] = rand.nextInt(1000);
        		num_zero[j] = 0;
        	}
           	dataWriter.append(new ClusterCenter(new Vector(num_zero)), new Vector(rand_vectors));
        }

        dataWriter.close();
	}

}

