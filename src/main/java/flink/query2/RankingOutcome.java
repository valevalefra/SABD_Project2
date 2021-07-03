package flink.query2;

import java.util.ArrayList;
import java.util.Date;

public class RankingOutcome {

    private final ArrayList<String> beforeRanking;
    private final ArrayList<String> afterRanking;
    private Date startDate;
    private String seaType;

    public RankingOutcome() {
        this.beforeRanking = new ArrayList<>();
        this.afterRanking = new ArrayList<>();
    }


    public void setBeforeRanking(String key) {
        this.beforeRanking.add(key);
    }

    public void setAfterRanking(String key) {
        this.afterRanking.add(key);
    }

    public ArrayList<String> getBeforeRanking() {
        return beforeRanking;
    }

    public ArrayList<String> getAfterRanking() {
        return afterRanking;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public String getSeaType() {
        return seaType;
    }

    public void setSeaType(String seaType) {
        this.seaType = seaType;
    }
}
