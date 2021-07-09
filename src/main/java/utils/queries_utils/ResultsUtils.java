package utils.queries_utils;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Utility Class to manage Results paths and to calculate windows offsets.
 */
public class ResultsUtils {

    public static final String QUERY1_WEEKLY_CSV_FILE_PATH = "Results/query1_weekly.csv";
    public static final String QUERY1_MONTHLY_CSV_FILE_PATH = "Results/query1_monthly.csv";
    public static final String QUERY2_WEEKLY_CSV_FILE_PATH = "Results/query2_weekly.csv";
    public static final String QUERY2_MONTHLY_CSV_FILE_PATH = "Results/query2_monthly.csv";
    public static final String QUERY3_1HOUR_CSV_FILE_PATH = "Results/query3_1hour.csv";
    public static final String QUERY3_2HOUR_CSV_FILE_PATH = "Results/query3_2hour.csv";
    public static final String PATH_JSON = "src/main/java/utils/queries_utils/dates.json";

    public static final String[] LIST_CSV = {QUERY1_WEEKLY_CSV_FILE_PATH, QUERY1_MONTHLY_CSV_FILE_PATH,
            QUERY2_WEEKLY_CSV_FILE_PATH, QUERY2_MONTHLY_CSV_FILE_PATH, QUERY3_1HOUR_CSV_FILE_PATH, QUERY3_2HOUR_CSV_FILE_PATH };

    public static String FIRST_JANUARY = "01-01 00:00";
    public static  Integer OFFSET_MONTHLY_OCCIDENTAL;
    public static Integer OFFSET_WEEKLY_OCCIDENTAL;
    public static Integer OFFSET_MONTHLY_TOTAL;
    public static Integer OFFSET_WEEKLY_TOTAL;

    // Function to clean Results folder
    public static void cleanFolder() {
        try {
            FileUtils.cleanDirectory(new File("Results"));
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Could not clean Results directory");
        }
    }

    /* Function to get weekly and monthly windows offsets;
       It is also considered the distinction between Occidental and Oriental
       Mediterranean Sea, to satisfy Query 1 and 2 requests.
     */
    public void takeOffsets() throws ParseException {

        long monthlySize = 28;
        long weeklySize = 7;

        JSONObject dates = readJson();
        String occidental = (String) dates.get("dateOccidental");
        String oriental= (String) dates.get("dateOriental");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        String firstJanuaryYear = occidental.substring(0,5);
        Date january = sdf.parse(firstJanuaryYear+FIRST_JANUARY);
        Date occidentalDate = sdf.parse(occidental);
        Date orientalDate = sdf.parse(oriental);
        OFFSET_WEEKLY_OCCIDENTAL = calculateDaysBetween(january, occidentalDate, weeklySize);
        OFFSET_MONTHLY_OCCIDENTAL = calculateDaysBetween(january, occidentalDate, monthlySize);
        if (orientalDate.compareTo(occidentalDate)<0) {
            OFFSET_WEEKLY_TOTAL = calculateDaysBetween(january, orientalDate, weeklySize);
            OFFSET_MONTHLY_TOTAL = calculateDaysBetween(january, orientalDate, monthlySize);
        }
        else{
            OFFSET_WEEKLY_TOTAL = OFFSET_WEEKLY_OCCIDENTAL;
            OFFSET_MONTHLY_TOTAL = OFFSET_MONTHLY_OCCIDENTAL;
        }
    }

    /* Function to calculate offset, by considering the difference between
       days from the 1st January to the given date of the same year, and the
       total window shifts until the given date.
     */
    private Integer calculateDaysBetween(Date january, Date firstDate, long size) {
        int i=0;
        long diff = firstDate.getTime()-january.getTime();
        long daysBetween = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
        while(size * i < daysBetween) i++;
        return (int) (daysBetween - (size * (i-1)));
    }



    // Function to read Json file
    public JSONObject readJson() {
        JSONParser jsonParser = new JSONParser();
        JSONObject datesList = new JSONObject();
        try (FileReader reader = new FileReader(PATH_JSON)) {
            //Read JSON file
            Object obj = jsonParser.parse(reader);
            datesList = (JSONObject) obj;

        } catch (IOException | org.json.simple.parser.ParseException e) {
            System.err.println("Dates file not found. Launch ProducerLauncher before.");
        }
        return datesList;
    }

}
