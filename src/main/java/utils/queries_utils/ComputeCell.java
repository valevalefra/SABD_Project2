package utils.queries_utils;
import java.util.Arrays;
import java.util.List;

public class ComputeCell {

    private static final double minLatitude = 32.0;
    private static final double maxLatitude = 45.0;
    private static final double minLongitude = -6.0;
    private static final double maxLongitude = 37.0;

    public static double getMinLatitude() {
        return minLatitude;
    }

    public static double getMaxLatitude() {
        return maxLatitude;
    }

    public static double getMinLongitude() {
        return minLongitude;
    }

    public static double getMaxLongitude() {
        return maxLongitude;
    }


    private static final double widthLat = (maxLatitude - minLatitude) / 10;
    private static final double widthLon = (maxLongitude - minLongitude) / 40;


    private static List<String> latSectors = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J");

    // Function to compute a sector
    public static String compute(double lat, double lon) {
        int posLat = (int) Math.ceil(((lat - minLatitude) / widthLat)-1);
        String latSector = latSectors.get(posLat);

        int lonSector = 1;
        int posLon = (int) ((lon - minLongitude) / widthLon);
        lonSector += posLon;

        return latSector + lonSector;
    }

}
