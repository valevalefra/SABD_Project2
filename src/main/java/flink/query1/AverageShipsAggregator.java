package flink.query1;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.ShipData;

import java.util.Date;

/**
 * Class implementing window aggregator for query1, in order to
 * obtain as result the sum of ships by type.
 */
public class AverageShipsAggregator implements AggregateFunction<ShipData, AverageShipsAccumulator, AverageOutcome> {


    @Override
    public AverageShipsAccumulator createAccumulator() {
        return new AverageShipsAccumulator();
    }

    @Override
    public AverageShipsAccumulator add(ShipData shipData, AverageShipsAccumulator averageShipsAccumulator) {
        averageShipsAccumulator.add(shipData.getType(), 1L);
        return averageShipsAccumulator;
    }


    @Override
    public AverageShipsAccumulator merge(AverageShipsAccumulator acc1, AverageShipsAccumulator acc2) {
        acc2.getMap().forEach(acc1::add);
        return acc1;
    }

    @Override
    public AverageOutcome getResult(AverageShipsAccumulator averageShipsAccumulator) {
        AverageOutcome outcome = new AverageOutcome();
        averageShipsAccumulator.getMap().forEach(outcome::addSum);
        return outcome;
    }
}
