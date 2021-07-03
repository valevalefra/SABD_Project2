package flink.query3;

import java.util.HashMap;
import java.util.List;

import static java.lang.Math.sqrt;

public class DistanceAccumulator {

    HashMap<String, DistanceCounter> distanceMap;

    public DistanceAccumulator() {
        this.distanceMap = new HashMap<>();
    }

    public void add(String idTrip, double latitude, double longitude) {

        DistanceCounter distances;
        if( distanceMap.get(idTrip)!=null){
            distances =  distanceMap.get(idTrip);
            double lastLong = distances.getLastLong();
            double lastLat = distances.getLastLat();
            //double longDist = Math.abs(longitude-lastLong);
            //double latDist = Math.abs(latitude-lastLat);
            double lastDistance = distances.getDistance();
            double distance = distance(latitude, lastLat, longitude, lastLong);
            double euclideanDistance = euclideanDistance(latitude, longitude, lastLat, lastLong);
           // System.out.println("distanza " + distance);
           // System.out.println("euclidea " + euclideanDistance);
           // double distance = Math.hypot(longDist, latDist);
           // System.out.println("lastLong " + lastLong + " lastLongAfter " + longitude + "lastLat " + lastLat +
            //        " lastLatAfter " + latitude + " distance " + distance );
            distances.setDistance(distance + lastDistance);

        }
        else{
            distances = new DistanceCounter();
            distances.setDistance(0.0);
        }
        distances.setLastLat(latitude);
        distances.setLastLong(longitude);
        distanceMap.put(idTrip, distances);
    }

    public void merge(HashMap<String, DistanceCounter> map) {

        this.distanceMap.putAll(map);


    }

    public HashMap<String, DistanceCounter> getDistanceMap() {
        return distanceMap;
    }

    public static double distance(double lat1, double lat2, double lon1,
                                  double lon2) {

        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters

        distance = Math.pow(distance, 2);

        return Math.sqrt(distance);
    }

    public static double euclideanDistance(double lat, double lng, double lat0, double lng0) {
        double deglen = 110.25;
        double x = lat - lat0;
        double y = (lng - lng0) * Math.cos(lat0);
        return deglen * sqrt(x * x + y * y) * 1000;
    }
}
