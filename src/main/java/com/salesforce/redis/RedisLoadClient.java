package com.salesforce.redis;

import redis.clients.jedis.Jedis;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.salesforce.redis.logging.Logger.log;
import static com.salesforce.redis.utils.Utils.*;

public class RedisLoadClient {

    private static final int RUN_TIME_ON_SECS = 300;
    private static final int DEFAULT_NO_OF_SORTED_SET_USERS = 3;
    private static final int DEFAULT_NO_OF_GENERAL_SET_USERS = 3;

    public static void main(String[] args) throws InterruptedException {
        log(String.valueOf(System.currentTimeMillis()));

        String sortedSetUserThreads = System.getProperty("SORTED_SET_USERS");
        int sortedSetUsersCount = sortedSetUserThreads != null ?
                Integer.parseInt(sortedSetUserThreads) :
                DEFAULT_NO_OF_SORTED_SET_USERS;
        String generalUserThreads = System.getProperty("GENERAL_USERS");
        int generalUsersCount = generalUserThreads != null ?
                Integer.parseInt(generalUserThreads) :
                DEFAULT_NO_OF_GENERAL_SET_USERS;

        String redisHost = System.getProperty("REDIS_HOST");
        String redisPort = System.getProperty("REDIS_PORT");
        Jedis jedis = new Jedis(URI.create("redis://" + redisHost + ":" + redisPort));
        String auth = System.getProperty("AUTH");
        if (auth != null) {
            jedis.auth(auth);
        }
        setup(jedis);
        long start = System.currentTimeMillis();

        List<RedisUser> sortedSetUsers = new ArrayList<>();
        for (int i = 1; i <= sortedSetUsersCount; i++) {
            RedisUser redisSortedSetUser = new RedisSortedSetUser(i);
            sortedSetUsers.add(redisSortedSetUser);
        }

        List<RedisUser> generalRedisUsers = new ArrayList<>();
        for (int i = 1; i <= generalUsersCount; i++) {
            RedisUser generalRedisUser = new GeneralRedisUser(i);
            generalRedisUsers.add(generalRedisUser);
        }

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleWithFixedDelay(new RedisMetricsPublisher(), 0, 1, TimeUnit.SECONDS);

        List<Runnable> tasks = new ArrayList<>();
        tasks.addAll(sortedSetUsers);
        tasks.addAll(generalRedisUsers);
        ExecutorService executorService = Executors.newFixedThreadPool(6);
        tasks.forEach(executorService::submit);

        Thread.sleep(RUN_TIME_ON_SECS * 1000L);
        executorService.shutdownNow();
        scheduledExecutorService.shutdownNow();
        scheduledExecutorService.awaitTermination(2000, TimeUnit.SECONDS);
        executorService.awaitTermination(2000, TimeUnit.SECONDS);

        long end = System.currentTimeMillis();
        printRunStats(sortedSetUsers, start, end);
        printRunStats(generalRedisUsers, start, end);
        printRedisCommandLatencies(jedis);
    }

    private static void printRedisCommandLatencies(Jedis jedis) {
        log("Command Latencies");
        log(jedis.info("latencystats"));
    }

    private static void printRunStats(List<RedisUser> users, long start, long end) {
        if (users.isEmpty()) {
            return;
        }

        log("Stats for: " + users.get(0).getUserType());

        long totalRunTime = TimeUnit.MILLISECONDS.toSeconds(end - start);
        log("Total Run Time in secs: " + totalRunTime);

        long totalOps = 0L;
        List<Long> latencies = new ArrayList<>();

        for (RedisUser user : users) {
            totalOps += user.getNoOfOps();
            log(user.getName() + ": " + user.getNoOfOps());
            latencies.addAll(user.getLatencies());
        }

        log("Total no of ops: " + totalOps);
        log("Throughout per sec: " + (totalOps / totalRunTime));
        printStats(latencies);
    }

    private static void setup(Jedis jedis) {
        jedis.flushDB();
        jedis.configResetStat();
        jedis.configSet("latency-tracking-info-percentiles", "50.0 75.0 90.0 99.0 99.9");
        jedis.del("set1");
        setupSetData(jedis);
        setupKVData(jedis);
    }

    private static void setupKVData(Jedis jedis) {
        for (int i = 0; i <= 10_000; i++) {
            String key = "key" + i;
            String value = generateRandomStringOfSize(50_000);
            jedis.set(key, value);
        }
    }

    private static void setupSetData(Jedis jedis) {

        List<String> dates = getDateStringsForLastNDays(300);
        List<String> values = new ArrayList<>();
        for (String date : dates) {
            for (int i = 1; i <= 10_000; i++) {
                values.add(date + ":" + generateRandomStringOfSize(18));
            }
        }

        insertDataIntoRedisSortedSet(values, jedis);
    }

    private static void insertDataIntoRedisSortedSet(List<String> values, Jedis jedis) {
        log("Inserting data into set");
        Map<String, Double> redisSortedSetMap = new HashMap<>();
        int count = 0;
        int batchSize = 10_000;
        for (String value : values) {
            if (count == batchSize) {
                addValuesToSet(redisSortedSetMap, jedis);
                redisSortedSetMap.clear();
                count = 0;
            }
            redisSortedSetMap.put(value, 0d);
            count++;
        }
        addValuesToSet(redisSortedSetMap, jedis);
    }

    private static void addValuesToSet(Map<String, Double> redisSortedSetMap, Jedis jedis) {
        jedis.zadd("set1", redisSortedSetMap);
    }

}
