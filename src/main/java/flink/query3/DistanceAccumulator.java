package flink.query3;

import java.util.HashMap;

public class DistanceAccumulator {

    HashMap<String, DistanceCounter> distanceMap;

    public DistanceAccumulator() {
        this.distanceMap = new HashMap<>();
    }

    // Function to identify the distance travelled for each tripId
    public void add(String idTrip, double latitude, double longitude) {

        DistanceCounter distances;
        /* If there is a distance travelled for the idTrip,
        the distance value is updated.
        */
        if( distanceMap.get(idTrip)!=null){
            distances =  distanceMap.get(idTrip);
            double lastLong = distances.getLastLong();
            double lastLat = distances.getLastLat();
            double lastDistance = distances.getDistance();
            double distance = distance(latitude, lastLat, longitude, lastLong);
            distances.setDistance(distance + lastDistance);
        }
        // If it's the first idTrip met, set the distance to 0
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

    // Distance calculation, based on the Haversine formula
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
}
