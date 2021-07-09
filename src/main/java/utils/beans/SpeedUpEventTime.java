package utils.beans;

public class SpeedUpEventTime implements Comparable<SpeedUpEventTime> {

    private final Long eventTime;
    private final String payload;


    public SpeedUpEventTime(Long eventTime, String payload) {
        this.eventTime = eventTime;
        this.payload = payload;
    }

    public long getEventTime() {
        return eventTime;
    }

    public String getPayload() {
        return payload;
    }


    @Override
    public int compareTo(SpeedUpEventTime other) {
        return Long.compare(getEventTime(), other.getEventTime());
    }
}
