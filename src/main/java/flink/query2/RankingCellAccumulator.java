package flink.query2;

import java.util.*;

public class RankingCellAccumulator {

    //key id cell value id ship
    private final HashMap<String, Set<String>> beforeMidDay;
    private final HashMap<String, Set<String>> afterMidDay;
    private final HashMap<String, Integer> frequencyMapBefore;
    private final HashMap<String, Integer> frequencyMapAfter;

    public RankingCellAccumulator() {

        this.beforeMidDay = new HashMap<>();
        this.afterMidDay = new HashMap<>();
        this.frequencyMapBefore = new HashMap<>();
        this.frequencyMapAfter = new HashMap<>();
    }


    public void add(String idShip, long timestamp, String idCell) {


        Calendar threshold = Calendar.getInstance();
        Date date = new Date(timestamp);
        threshold.setTime(date);

        // current event date setup
        Calendar elem = Calendar.getInstance();
        elem.setTime(date);

        threshold.set(Calendar.MINUTE, 0);
        threshold.set(Calendar.SECOND, 0);
        threshold.set(Calendar.MILLISECOND, 0);

        // set threshold at 12:00 of the same day
        threshold.set(Calendar.HOUR_OF_DAY, 12);

        //check if it falls in am or pm
        if (elem.before(threshold)) {
            // add to morning ranking
            Set<String> set = this.beforeMidDay.get(idCell);
            if(set == null) set = new HashSet<>();
            set.add(idShip);
            this.beforeMidDay.put(idCell, set);
            this.frequencyMapBefore.put(idCell, this.beforeMidDay.get(idCell).size());

        } else {
            // add to afternoon ranking
            Set<String> set = this.afterMidDay.get(idCell);
            if(set == null) set = new HashSet<>();
            set.add(idShip);
            this.afterMidDay.put(idCell, set);
            this.frequencyMapAfter.put(idCell, this.afterMidDay.get(idCell).size());
        }


    }

    public HashMap<String, Set<String>> getBeforeMidDay() {
        return beforeMidDay;
    }

    public HashMap<String, Set<String>> getAfterMidDay() {
        return afterMidDay;
    }

    public HashMap<String, Integer> getFrequencyMapBefore() {
        return frequencyMapBefore;
    }

    public HashMap<String, Integer> getFrequencyMapAfter() {
        return frequencyMapAfter;
    }

    public void merge(HashMap<String, Set<String>> beforeMidDay, HashMap<String, Set<String>> afterMidDay) {

        beforeMidDay.forEach((k,v) -> v.addAll(this.beforeMidDay.get(k)));
        afterMidDay.forEach((k,v) -> v.addAll(this.afterMidDay.get(k)));
    }
}
