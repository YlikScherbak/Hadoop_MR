package geolocation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GeolocationMapper extends Mapper<LongWritable, Text, Text, GeolocationData>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");


        if (line.length == 10) {
            context.write(new Text(line[0]), new GeolocationData(line[6], line[5], Integer.parseInt(line[7])));
        }
    }
}
