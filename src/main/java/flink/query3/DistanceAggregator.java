package flink.query3;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.beans.ShipData;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Class implementing window aggregator for query 3 in order to
 * obtain as result the first 5 tripIDs that have traveled the longest distance
 */
public class DistanceAggregator implements AggregateFunction<ShipData, DistanceAccumulator, DistanceOutcome> {
    @Override
    public DistanceAccumulator createAccumulator() {
        return new DistanceAccumulator();
    }

    @Override
    public DistanceAccumulator add(ShipData shipData, DistanceAccumulator distanceAccumulator) {
        distanceAccumulator.add(shipData.getIdTrip(), shipData.getLatitude(), shipData.getLongitude());
        return distanceAccumulator;
    }

    @Override
    public DistanceAccumulator merge(DistanceAccumulator acc1, DistanceAccumulator acc2) {
        acc1.merge(acc2.getDistanceMap());
        return acc1;
    }

    @Override
    public DistanceOutcome getResult(DistanceAccumulator distanceAccumulator) {
        // Create the list from elements of HashMap
        List<Map.Entry<String, DistanceCounter>> distanceList = new LinkedList<>(distanceAccumulator.getDistanceMap().entrySet());

        // Sort the list in descending order
        distanceList.sort((o1, o2) -> o2.getValue().getDistance().compareTo(o1.getValue().getDistance()));

        DistanceOutcome outcome = new DistanceOutcome();
        if(distanceList.size()>=5) {
            for (int i = 0; i < 5; i++) {
                try {
                    outcome.setRankingByDistance(distanceList.get(i).getKey(), distanceList.get(i).getValue().getDistance());
                } catch (IndexOutOfBoundsException ignored) {
                    System.err.println("Wrong number of elements in list");
                }

            }
        }
        else{
            for(int i = 0; i<distanceList.size(); i++ ){
                try {
                    outcome.setRankingByDistance(distanceList.get(i).getKey(), distanceList.get(i).getValue().getDistance());
                } catch (IndexOutOfBoundsException ignored) {
                    System.err.println("Wrong number of elements in list");
                }
            }
        }
        return outcome;
    }
}
