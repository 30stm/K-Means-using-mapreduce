package mapreduce.MapreduceKMeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import Tools.ClusterCenter;
import Tools.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

// calculate a new clustercenter for these vertices

public class KMeansReducer extends
		Reducer<ClusterCenter, Vector, ClusterCenter, Vector> {

//	private final List<ClusterCenter> centers = new ArrayList<>();
    public static enum Counter {
        CONVERGED
}
	List<ClusterCenter> centers = new ArrayList<ClusterCenter>();

	
	@Override
	protected void reduce(ClusterCenter key, Iterable<Vector> values,
			Context context) throws IOException, InterruptedException {

//		List<Vector> vectorList = new LinkedList<Vector>();
		List<Vector> vectorList = new ArrayList<Vector>();
		Vector newCenter = new Vector();

//		int vectorSize = key.getCenter().getVector().length;
		newCenter.setVector(new double[key.getCenter().getVector().length]);
//		 
//		
		for (Vector value : values) {
			vectorList.add(new Vector(value));
			System.out.println(value);
//			if(newCenter == null)
//				newCenter.setVector(value.getVector());
//			else
////				newCenter.set = newCenter.add(value.getVector());
//				newCenter.setVector(newCenter.add(value.getVector()));
//		}
//		newCenter = newCenter.divide(vectorList.size());
		
			for (int i = 0; i < value.getVector().length; i++) {
				newCenter.getVector()[i] += value.getVector()[i];
			}
		}
		
		newCenter.setVector(newCenter.divide(vectorList.size()));
		
		ClusterCenter center = new ClusterCenter(newCenter);
		centers.add(center);
		
//		System.out.println(center.toString());	//
		
		for (Vector vector : vectorList) {
			context.write(center, vector);
		}

		if (center.converged(key))
			context.getCounter(Counter.CONVERGED).increment(1);
	}

	@SuppressWarnings("deprecation")
	@Override
	protected void cleanup(Context context) throws IOException,	InterruptedException {
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		Path outPath = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);
		
		SequenceFile.Writer out = SequenceFile.createWriter(fs,	context.getConfiguration(), outPath, ClusterCenter.class,IntWritable.class);
		final IntWritable value = new IntWritable(0);
		for (ClusterCenter center : centers) {
			out.append(center, value);
		}
//			out.close();

	}
}
