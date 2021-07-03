package utils;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

public class MonthlyWindowAssigner extends TumblingEventTimeWindows {

    public MonthlyWindowAssigner() {
        super(1, 0);
    }

    /**
     * Returns a {@code Collection} of windows that should be assigned to the element.
     *
     * @param element The element to which windows should be assigned.
     * @param timestamp The timestamp of the element.
     * @param context The {@link WindowAssignerContext} in which the assigner operates.
     */

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        Calendar calendar = Calendar.getInstance();

        calendar.setTime(new Date(timestamp));
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);

        long startDate = calendar.getTimeInMillis();

        calendar.add(Calendar.MONTH, 1);

        long endDate = calendar.getTimeInMillis() - 1;

        return Collections.singletonList(new TimeWindow(startDate, endDate));

    }
}
