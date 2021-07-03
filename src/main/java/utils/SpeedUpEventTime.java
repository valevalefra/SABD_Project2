package utils;

public class SpeedUpEventTime implements Comparable<SpeedUpEventTime> {

    private Long eventTime;
    private String payload;


    public SpeedUpEventTime(Long eventTime, String payload) {
        this.eventTime = eventTime;
        this.payload = payload;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }



    @Override
    public int compareTo(SpeedUpEventTime other) {
        return Long.compare(getEventTime(), other.getEventTime());
    }
}
