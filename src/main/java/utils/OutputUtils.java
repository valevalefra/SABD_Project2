package utils;

import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class OutputUtils {

    public static final String QUERY1_WEEKLY_CSV_FILE_PATH = "Results/query1_weekly.csv";
    public static final String QUERY1_MONTHLY_CSV_FILE_PATH = "Results/query1_monthly.csv";
    public static final String QUERY2_WEEKLY_CSV_FILE_PATH = "Results/query2_weekly.csv";
    public static final String QUERY2_MONTHLY_CSV_FILE_PATH = "Results/query2_monthly.csv";
    public static final String QUERY3_1HOUR_CSV_FILE_PATH = "Results/query3_1hour.csv";
    public static final String QUERY3_2HOUR_CSV_FILE_PATH = "Results/query3_2hour.csv";

    public static final String[] LIST_CSV = {QUERY1_WEEKLY_CSV_FILE_PATH, QUERY1_MONTHLY_CSV_FILE_PATH,
            QUERY2_WEEKLY_CSV_FILE_PATH, QUERY2_MONTHLY_CSV_FILE_PATH, QUERY3_1HOUR_CSV_FILE_PATH, QUERY3_2HOUR_CSV_FILE_PATH };

    public static Date FIRST_DATE = null;
    public static String FIRST_JANUARY = "01-01-2015";
    public static  Integer OFFSET_MONTHLY;
    public static Integer OFFSET_WEEKLY;


    public static Integer getOffsetMonthly() {
        return OFFSET_MONTHLY;
    }

    public static void setOffsetMonthly(Integer offsetMonthly) {
        OFFSET_MONTHLY = offsetMonthly;
    }

    public static Integer getOffsetWeekly() {
        return OFFSET_WEEKLY;
    }

    public static void setOffsetWeekly(Integer offsetWeekly) {
        OFFSET_WEEKLY = offsetWeekly;
    }

    public static void cleanFolder() {
        try {
            FileUtils.cleanDirectory(new File("Results"));
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Could not clean Results directory");
        }
    }


    public static Integer takeOffset(Stream<Date> stream) throws ParseException {

        FIRST_DATE = stream.findFirst()
                .orElse(null);
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
        Date january = sdf.parse(FIRST_JANUARY);
        long diff = FIRST_DATE.getTime()-january.getTime();
        long daysBetween = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
        int monthlySize = 28;
        int weeklySize = 7;
        int i = 0;
        int j = 0;
        while(monthlySize * i < daysBetween){
            i++;
        }
        while(weeklySize * j < daysBetween){
            j++;
        }
        setOffsetMonthly((int) (daysBetween - (monthlySize * (i-1))+1));
        return OFFSET_WEEKLY = (int) (daysBetween - (weeklySize * (j-1))+1);
        //OFFSET_MONTHLY = (int) (daysBetween - (monthlySize * (i-1))+1);

       // System.out.println(OFFSET_MONTHLY);
        //System.out.println(OFFSET_WEEKLY);


    }

}
