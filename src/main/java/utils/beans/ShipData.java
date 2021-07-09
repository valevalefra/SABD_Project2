package utils.beans;

import utils.queries_utils.ComputeCell;

public class ShipData implements Comparable<ShipData>{

    private static final double canaleDiSiciliaLon = 11.797696;
    private double longitude;
    private double latitude;
    private String type;
    private String idCell;
    private long timestamp;
    private String seaType;
    private String idShip;
    private String idTrip;



    public ShipData(double longitude, double latitude, Long timestamp, String idShip){
        this.longitude = longitude;
        this.latitude = latitude;
        this.timestamp = timestamp;
        this.idCell = ComputeCell.compute(latitude, longitude);
        this.seaType = this.defineSeaType(this.longitude);
        this.idShip = idShip;
    }

    public ShipData(double longitude, double latitude,String idTrip, Long timestamp){
        this.longitude = longitude;
        this.latitude = latitude;
        this.timestamp = timestamp;
        this.idTrip = idTrip;
    }

    public ShipData(double longitude, double latitude, int type, Long timestamp){
        this.longitude = longitude;
        this.latitude = latitude;
        this.type = this.defineType(type);
        this.timestamp = timestamp;
        this.idCell = ComputeCell.compute(latitude, longitude);
    }


    private String defineSeaType(double longitude) {

        if(longitude<=canaleDiSiciliaLon){
            return "Occidental";
        }
        else{
            return "Oriental";
        }
    }

    public ShipData(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    private static String defineType(int type){

        if(type == 35){
            return "MILITARY";
        }
        else if(type >= 60 && type <= 69){
            return "PASSENGER";
        }
        else if(type >= 70 && type <= 79){
            return "CARGO";
        }
        else{
            return "OTHER";
        }
    }

    public String getSeaType() {
        return seaType;
    }

    public void setSeaType(String seaType) {
        this.seaType = seaType;
    }

    public String getIdShip() {
        return idShip;
    }

    public void setIdShip(String idShip) {
        this.idShip = idShip;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getIdCell() {
        return idCell;
    }

    public void setIdCell(String idCell) {
        this.idCell = idCell;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getIdTrip() {
        return idTrip;
    }

    public void setIdTrip(String idTrip) {
        this.idTrip = idTrip;
    }

    @Override
    public String toString() {
        return "longitude=" + longitude +
                ", latitude=" + latitude +
                ", type='" + type + '\'' +
                ", idCell='" + idCell + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public int compareTo(ShipData other) {
        return Long.compare(this.timestamp, other.timestamp);
    }
}
