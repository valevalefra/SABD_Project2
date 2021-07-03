package flink.query1;

public class Query1Result {

    private Double avg35 = 0.0;
    private Long ship35 = 0L;
    private Double avg6069 = 0.0;
    private Long ship6069 = 0L;
    private Double avg7079 = 0.0;
    private Long ship7079 = 0L;
    private Double avgO = 0.0;
    private Long shipO = 0L;


    public Query1Result() {

    }

    @Override
    public String toString() {
        return "Query1Result{" +
                "avg35=" + avg35 +
                ", ship35=" + ship35 +
                ", avg6069=" + avg6069 +
                ", ship6069=" + ship6069 +
                ", avg7079=" + avg7079 +
                ", ship7079=" + ship7079 +
                ", avgO=" + avgO +
                ", shipO=" + shipO +
                '}';
    }



    public void switchType(String type, Double avg, Long total){
        switch (type){
            case "MILITARY":
                setAvg35(avg);
                setShip35(total);
                break;
            case "PASSENGER":
                setAvg6069(avg);
                setShip6069(total);
                break;
            case "CARGO":
                setAvg7079(avg);
                setShip7079(total);
                break;
            case "OTHER":
                setAvgO(avg);
                setShipO(total);
                break;

        }
    }

    public Double getAvg35() {
        return avg35;
    }

    public void setAvg35(Double avg35) {
        this.avg35 = avg35;
    }

    public Long getShip35() {
        return ship35;
    }

    public void setShip35(Long ship35) {
        this.ship35 = ship35;
    }

    public Double getAvg6069() {
        return avg6069;
    }

    public void setAvg6069(Double avg6069) {
        this.avg6069 = avg6069;
    }

    public Long getShip6069() {
        return ship6069;
    }

    public void setShip6069(Long ship6069) {
        this.ship6069 = ship6069;
    }

    public Double getAvg7079() {
        return avg7079;
    }

    public void setAvg7079(Double avg7079) {
        this.avg7079 = avg7079;
    }

    public Long getShip7079() {
        return ship7079;
    }

    public void setShip7079(Long ship7079) {
        this.ship7079 = ship7079;
    }

    public Double getAvgO() {
        return avgO;
    }

    public void setAvgO(Double avgO) {
        this.avgO = avgO;
    }

    public Long getShipO() {
        return shipO;
    }

    public void setShipO(Long shipO) {
        this.shipO = shipO;
    }
}