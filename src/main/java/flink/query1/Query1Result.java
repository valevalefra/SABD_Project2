package flink.query1;

public class Query1Result {

    private Double avg35 = 0.0;
    private Double avg6069 = 0.0;
    private Double avg7079 = 0.0;
    private Double avgO = 0.0;


    public Query1Result() {

    }

    @Override
    public String toString() {
        return "Query1Result{" +
                "avg35=" + avg35 +
                ", avg6069=" + avg6069 +
                ", avg7079=" + avg7079 +
                ", avgO=" + avgO +
                '}';
    }


    // Function to set the right variable with respect to the type
    public void switchType(String type, Double avg){
        switch (type){
            case "MILITARY":
                setAvg35(avg);
                break;
            case "PASSENGER":
                setAvg6069(avg);
                break;
            case "CARGO":
                setAvg7079(avg);
                break;
            case "OTHER":
                setAvgO(avg);
                break;

        }
    }

    public Double getAvg35() {
        return avg35;
    }

    public void setAvg35(Double avg35) {
        this.avg35 = avg35;
    }

    public Double getAvg6069() {
        return avg6069;
    }

    public void setAvg6069(Double avg6069) {
        this.avg6069 = avg6069;
    }

    public Double getAvg7079() {
        return avg7079;
    }

    public void setAvg7079(Double avg7079) {
        this.avg7079 = avg7079;
    }

    public Double getAvgO() {
        return avgO;
    }

    public void setAvgO(Double avgO) {
        this.avgO = avgO;
    }

}