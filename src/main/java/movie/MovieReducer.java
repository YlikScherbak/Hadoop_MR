package movie;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MovieReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<Text, Integer> map = new HashMap<>(100);

        for (Text genre: values) {
            if (map.containsKey(genre)) {
                map.put(new Text(genre), map.get(genre) + 1);
            }else {
                map.put(new Text(genre), 1);
            }
        }

        for (Map.Entry<Text, Integer> entry: map.entrySet()) {
            Text genre = new Text("(" + entry.getKey() + " " +  entry.getValue().toString()+ ")");
            context.write(key, genre);
        }
        map.clear();
    }
}
