package geolocation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GeolocationData implements Writable {

    private Text state;
    private Text city;
    private DoubleWritable velocity;


    public GeolocationData() {
        this.state = new Text();
        this.city = new Text();
        this.velocity = new DoubleWritable();
    }

    public GeolocationData(Text state, Text city, DoubleWritable velocity) {
        this.state = state;
        this.city = city;
        this.velocity = velocity;
    }

    public GeolocationData(String state, String city, Integer velocity) {
        this.state = new Text(state);
        this.city = new Text(city);
        this.velocity = new DoubleWritable(velocity);
    }


    public GeolocationData(GeolocationData data){
        state = new Text(data.getState());
        city = new Text(data.getCity());
        velocity = new DoubleWritable(Double.parseDouble(data.getVelocity().toString()));
    }

    public Text getState() {
        return state;
    }

    public void setState(Text state) {
        this.state = state;
    }

    public Text getCity() {
        return city;
    }

    public void setCity(Text city) {
        this.city = city;
    }

    public DoubleWritable getVelocity() {
        return velocity;
    }

    public void setVelocity(DoubleWritable velocity) {
        this.velocity = velocity;
    }

    public void write(DataOutput out) throws IOException {
        state.write(out);
        city.write(out);
        velocity.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        state.readFields(in);
        city.readFields(in);
        velocity.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeolocationData that = (GeolocationData) o;

        if (state != null ? !state.toString().equals(that.state.toString()) : that.state != null) return false;
        return city != null ? city.toString().equals(that.city.toString()) : that.city == null;
    }

    @Override
    public int hashCode() {
        int result = state != null ? state.hashCode() : 0;
        result = 31 * result + (city != null ? city.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return state.toString() + " " + city.toString() + " " + velocity.toString();
    }
}
