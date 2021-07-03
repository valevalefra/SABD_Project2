package flink.query2;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.ShipData;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RankingCellAggregator implements AggregateFunction<ShipData, RankingCellAccumulator, RankingOutcome> {
    @Override
    public RankingCellAccumulator createAccumulator() {
        return new RankingCellAccumulator();
    }

    @Override
    public RankingCellAccumulator add(ShipData shipData, RankingCellAccumulator rankingCellAccumulator) {
        rankingCellAccumulator.add(shipData.getIdShip(), shipData.getTimestamp(), shipData.getIdCell());
        return rankingCellAccumulator;
    }

    @Override
    public RankingCellAccumulator merge(RankingCellAccumulator acc1, RankingCellAccumulator acc2) {
        acc1.merge(acc2.getBeforeMidDay(), acc2.getAfterMidDay());
        return acc1;
    }

    @Override
    public RankingOutcome getResult(RankingCellAccumulator rankingCellAccumulator) {

        // Create the lists from elements of HashMap
        List<Map.Entry<String, Integer>> beforeList = new LinkedList<>(rankingCellAccumulator.getFrequencyMapBefore().entrySet());
        List<Map.Entry<String, Integer>> afterList = new LinkedList<>(rankingCellAccumulator.getFrequencyMapAfter().entrySet());

        // Sort the lists in descending order
        beforeList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        afterList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        RankingOutcome outcome = new RankingOutcome();
        if(beforeList.size()>=3 && afterList.size()>=3) {
            for (int i = 0; i < 3; i++) {
                try {
                    outcome.setBeforeRanking(beforeList.get(i).getKey());
                } catch (IndexOutOfBoundsException ignored) {
                    // Less than RANK_SIZE elements
                }

                try {
                    outcome.setAfterRanking(afterList.get(i).getKey());
                } catch (IndexOutOfBoundsException ignored) {
                    // Less than RANK_SIZE elements
                }
            }
        }
        else{
            for(int i = 0; i<beforeList.size(); i++ ){
                try {
                    outcome.setBeforeRanking(beforeList.get(i).getKey());
                } catch (IndexOutOfBoundsException ignored) {
                    // Less than RANK_SIZE elements
                }
            }
            for(int i = 0; i<afterList.size(); i++ ){
                try {
                    outcome.setAfterRanking(afterList.get(i).getKey());
                } catch (IndexOutOfBoundsException ignored) {
                    // Less than RANK_SIZE elements
                }
            }
        }
       // System.out.println("after " + outcome.getAfterRanking());
       // System.out.println("before "+ outcome.getBeforeRanking());
        return outcome;

    }

}
