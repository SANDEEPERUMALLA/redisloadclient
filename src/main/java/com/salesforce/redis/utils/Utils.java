package com.salesforce.redis.utils;

import com.google.common.math.Stats;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.math.Quantiles.percentiles;
import static com.salesforce.redis.logging.Logger.log;

public class Utils {

    private Utils(){}

    public static String getDateString(LocalDateTime localDateTime) {
        String month = sanitize(localDateTime.getMonth().getValue());
        String day = sanitize(localDateTime.getDayOfMonth());
        String year = String.valueOf(localDateTime.getYear());
        return month + "/" + day + "/" + year;
    }

    private static String sanitize(int d) {
        String day = String.valueOf(d);
        return day.length() == 1 ? "0" + day : day;
    }

    public static String generateRandomStringOfSize(int size) {
        String s = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder str = new StringBuilder();
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        for (int i = 1; i <= size; i++) {
            str.append(s.charAt(threadLocalRandom.nextInt(0, s.length() - 1)));
        }
        return str.toString();
    }

    public static void printStats(List<Long> times) {
        double p99 = percentiles().index(99).compute(times);
        double p90 = percentiles().index(90).compute(times);
        double p75 = percentiles().index(50).compute(times);
        double p50 = percentiles().index(75).compute(times);
        double average = Stats.meanOf(times);
        log("Stats");
        printTime("P99", (long) p99);
        printTime("P90", (long) p90);
        printTime("P75", (long) p75);
        printTime("P50", (long) p50);
        printTime("Average", (long) average);
    }

    private static void printTime(String timeIdentifier, long timeInNs) {
        log(timeIdentifier + ": " + TimeUnit.NANOSECONDS.toMillis(timeInNs) + " ms," + TimeUnit.NANOSECONDS.toMicros(timeInNs) + " us");
    }

    private static void printTime(long timeInNs) {
        printTime("Time: ", timeInNs);
    }

    public static List<String> getDateStringsForLastNDays(int n) {
        LocalDateTime dateTime = LocalDateTime.now();
        List<String> dateStrings = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            dateStrings.add(getDateString(dateTime));
            dateTime = dateTime.minusDays(1);
        }
        return dateStrings;
    }


}
