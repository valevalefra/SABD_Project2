package flink.query1;

import java.util.HashMap;

public class AverageShipsAccumulator {

    private HashMap<String, Long> shipMap;

    public AverageShipsAccumulator() {
        this.shipMap = new HashMap<>();
    }

    /* Function to merge records with the same ship type
     * adding their counter.
     */
    public void add(String type, long counter){
        this.shipMap.merge(type, counter, Long::sum);
    }

    public HashMap<String, Long> getMap() {
        return shipMap;
    }

}
