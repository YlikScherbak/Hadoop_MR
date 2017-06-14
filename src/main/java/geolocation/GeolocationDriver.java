package geolocation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GeolocationDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new GeolocationDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("Usage: %s [generic options] <inputdir> <outputdir>\n",
                    getClass().getSimpleName());
            return -1;
        }
        Job job = new Job(getConf());
        job.setJarByClass(GeolocationData.class);
        job.setJobName("Geolocation");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(GeolocationMapper.class);
        job.setReducerClass(GeolocationReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(GeolocationData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GeolocationData.class);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
