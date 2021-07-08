package flink.query3;

public class DistanceCounter {

    private Double distance;
    private Double lastLong;
    private Double lastLat;

    public DistanceCounter(Double distance, Double lastLong, Double lastLat) {
        this.distance = distance;
        this.lastLong = lastLong;
        this.lastLat = lastLat;
    }

    public DistanceCounter() {

    }

    public Double getDistance() {
        return distance;
    }

    public void setDistance(Double distance) {
        this.distance = distance;
    }

    public Double getLastLong() {
        return lastLong;
    }

    public void setLastLong(Double lastLong) {
        this.lastLong = lastLong;
    }

    public Double getLastLat() {
        return lastLat;
    }

    public void setLastLat(Double lastLat) {
        this.lastLat = lastLat;
    }

}

