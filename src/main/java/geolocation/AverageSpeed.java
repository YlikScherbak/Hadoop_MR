package geolocation;

import org.apache.hadoop.io.DoubleWritable;

public class AverageSpeed {

    private Double velocity = 0D;

    private int counter = 0;

    public AverageSpeed() {
    }

    public AverageSpeed(Double d) {
        velocity += d;
        counter++;
    }

    public AverageSpeed(GeolocationData data) {
        this.velocity += data.getVelocity().get();
        this.counter++;
    }

    public Double getVelocity() {
        return velocity;
    }

    public void setVelocity(Double velocity) {
        this.velocity = velocity;
    }

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }

    public AverageSpeed add(GeolocationData data){
        this.velocity += data.getVelocity().get();
        this.counter++;
        return this;
    }

    public AverageSpeed add(Double data){
        this.velocity += data;
        this.counter++;
        return this;
    }

    public DoubleWritable averageVelocity(){
        return new DoubleWritable(velocity / counter);
    }
}
