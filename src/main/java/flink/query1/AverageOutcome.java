package flink.query1;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class AverageOutcome {

    private Date startDate;
    private final HashMap<String, Long> shipMeans = new HashMap<>();
    private List<Query1Result> list;
    private String idCell;

    public List<Query1Result> getList() {
        return list;
    }

    public void setList(List<Query1Result> list) {
        this.list = list;
    }

    public String getIdCell() {
        return idCell;
    }

    public void setIdCell(String idCell) {
        this.idCell = idCell;
    }

    public AverageOutcome() {
    }

    public void addSum(String type, Long sum) {
        if (type.equals("")) {
            return;
        }
        this.shipMeans.put(type, sum);
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public HashMap<String, Long> getShipMeans() {

        return shipMeans;
    }

    @Override
    public String toString() {
        return "AverageOutcome{" +
                "startDate=" + startDate +
                ", shipMeans=" + shipMeans +
                ", list=" + list +
                ", idCell='" + idCell + '\'' +
                '}';
    }
}

