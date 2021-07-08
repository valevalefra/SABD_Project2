package flink.query3;




import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class DistanceOutcome {

    private final List<Tuple2<String, Double>> ranking;
    private Date startDate;


    public DistanceOutcome() {
        this.ranking = new ArrayList<>();
    }

    public void setRankingByDistance(String key, Double value) {

        Tuple2<String, Double> tuple = new Tuple2<>(key, value);
        this.ranking.add(tuple);
    }

    public List<Tuple2<String, Double>> getRanking() {
        return ranking;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date date) {
        this.startDate = date;
    }
}
