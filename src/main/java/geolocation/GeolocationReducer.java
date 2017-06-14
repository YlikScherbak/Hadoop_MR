package geolocation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GeolocationReducer extends Reducer<Text, GeolocationData, Text, GeolocationData> {

    @Override
    protected void reduce(Text key, Iterable<GeolocationData> values, Context context) throws IOException, InterruptedException {
        Map<GeolocationData, AverageSpeed> map = new HashMap<>(50);

        for (GeolocationData data : values) {
            if (map.containsKey(data)) {
                map.put(new GeolocationData(data), map.get(data).add(data));
            } else {
                map.put(new GeolocationData(data), new AverageSpeed(data));
            }
        }

        for (Map.Entry<GeolocationData, AverageSpeed> entry : map.entrySet()) {
            GeolocationData data = new GeolocationData(entry.getKey().getState(), entry.getKey().getCity(), entry.getValue().averageVelocity());
            context.write(key, data);
        }
        map.clear();
    }
}
