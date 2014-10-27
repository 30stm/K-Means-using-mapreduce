
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import Tools.ClusterCenter;
import Tools.Vector;

public class KMeansClusteringJob {
    private static final Log LOG = LogFactory.getLog(KMeansClusteringJob.class);

                @SuppressWarnings("deprecation")
				public static void main(String[] args) throws IOException,
                        InterruptedException, ClassNotFoundException {

                int max_iteration =  Integer.parseInt(args[0]);
//                int dimension =   Integer.parseInt(args[1]);
//                int clusters =    Integer.parseInt(args[1]);
                
                int iteration = 0;
                Configuration conf = new Configuration();
                
                conf.set("num.iteration", iteration + "");
//                conf.setInt("dimension", dimension);
//                conf.setInt("clusters", clusters);
                
                Path in = new Path("MRKmeans/clustering/import/data");
                Path center = new Path("MRKmeans/clustering/import/center/cen.seq");
                conf.set("centroid.path", center.toString());
                
//                int nextiter = iteration+1;
//                Path nextcenter = new Path("MRKmeans/clustering/import/center/cen"+nextiter+".seq");
//                conf.set("nextcentroid.path", nextcenter.toString());

                Path out = new Path("MRKmeans/clustering/depth_1");

                Job job = new Job(conf);
                job.setJobName("KMeans Clustering");

                job.setMapperClass(KMeansMapper.class);
                job.setReducerClass(KMeansReducer.class);
                job.setJarByClass(KMeansMapper.class);

//                SequenceFileInputFormat.addInputPath(job, in);
                FileInputFormat.addInputPath(job, in);
                FileSystem fs = FileSystem.get(conf);
                
                if (fs.exists(out))
                        fs.delete(out, true);

                if (fs.exists(center))
                        fs.delete(out, true);

                if (fs.exists(in))
                        fs.delete(out, true);
                
//                SequenceFileOutputFormat.setOutputPath(job, out);
                FileOutputFormat.setOutputPath(job, out);
                job.setInputFormatClass(SequenceFileInputFormat.class);
                job.setOutputFormatClass(SequenceFileOutputFormat.class);
                
                job.setOutputKeyClass(ClusterCenter.class);
                job.setOutputValueClass(Vector.class);
                
                job.waitForCompletion(true);
                long counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
                iteration++;
                
                
                while (iteration < max_iteration) {
                        conf = new Configuration();
                        conf.set("centroid.path", center.toString());
//                        Path currentcenter = new Path("MRKmeans/clustering/import/center/cen"+iteration+".seq");
//                        conf.set("centroid.path", currentcenter.toString());
//                        conf.set("num.iteration", iteration + "");
//                        conf.set("nextcentroid.path", nextcenter.toString());
                        
                        job = new Job(conf);
                        job.setJobName("KMeans Clustering " + iteration);

                        job.setMapperClass(KMeansMapper.class);
                        job.setReducerClass(KMeansReducer.class);
                        job.setJarByClass(KMeansMapper.class);
                        
                        in = new Path("MRKmeans/clustering/depth_" + (iteration - 1) + "/");
                        out = new Path("MRKmeans/clustering/depth_" + iteration);

//                        SequenceFileInputFormat.addInputPath(job, in);
                        FileInputFormat.addInputPath(job, in);
                        if (fs.exists(out))
                                fs.delete(out, true);

                        FileOutputFormat.setOutputPath(job, out);
//                        SequenceFileOutputFormat.setOutputPath(job, out);
                        job.setInputFormatClass(SequenceFileInputFormat.class);
                        job.setOutputFormatClass(SequenceFileOutputFormat.class);
                        job.setOutputKeyClass(ClusterCenter.class);
                        job.setOutputValueClass(Vector.class);

                        job.waitForCompletion(true);
                        
                        iteration++;
                        counter = job.getCounters()
                                .findCounter(KMeansReducer.Counter.CONVERGED).getValue();
                }

                Path result = new Path("MRKmeans/clustering/depth_" + (iteration - 1) + "/");

                BufferedWriter writer = new BufferedWriter(new FileWriter("result.txt"));
                FileStatus[] stati = fs.listStatus(result);
                for (FileStatus status : stati) {
                        if (!status.isDir()) {
                                Path path = status.getPath();
                                if(!path.getName().equals("_SUCCESS")){
//                                	LOG.info("FOUND " + path.toString());
                                	SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
                                	ClusterCenter key = new ClusterCenter();
                                	Vector v = new Vector();
                                	while (reader.next(key, v)) {
//                                		LOG.info(key + " / " + v);
//                                		writer.write("key:"+key+"value:"+v);
                                	}
                                	reader.close();
                                }
                        }
                }
        }

}
