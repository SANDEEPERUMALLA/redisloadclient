package com.salesforce.redis;

import redis.clients.jedis.Jedis;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.salesforce.redis.logging.Logger.log;
import static com.salesforce.redis.utils.Utils.getDateString;

public class RedisSortedSetUser implements RedisUser {

    private final Jedis jedis;
    private long noOfOps = 0;
    List<Long> latencies = new ArrayList<>();
    private final int userId;
    private static final String REDIS_SORTED_USER_FORMAT = "redis-sorted-set-user";

    public RedisSortedSetUser(int userId) {
        this.userId = userId;
        String redisHost = System.getProperty("REDIS_HOST");
        String redisPort = System.getProperty("REDIS_PORT");
        this.jedis = new Jedis(URI.create("redis://" + redisHost+ ":" + redisPort));
        String auth = System.getProperty("AUTH");
        if(auth != null) {
            jedis.auth(auth);
        }
    }

    @Override
    public void run() {
        Thread.currentThread().setName(getName());
        while (!Thread.currentThread().isInterrupted()) {
            LocalDateTime dateTime = LocalDateTime.now();
            for (int i = 1; i <= 200; i++) {
                String eR = getDateString(dateTime);
                dateTime = dateTime.minusDays(1);
                String sR = getDateString(dateTime);
                log("Range : [" + sR + " - " + eR + "]");
                long start = System.nanoTime();
                Set<String> result = jedis.zrangeByLex("set1", "[" + sR, "[" + eR);
                long time = System.nanoTime() - start;
                noOfOps++;
                log("ZRange Op Time: " + time);
                latencies.add(time);
                log("Result Size : " + result.size());
            }
        }
    }

    @Override
    public long getNoOfOps() {
        return noOfOps;
    }

    @Override
    public List<Long> getLatencies() {
        return latencies;
    }

    @Override
    public String getName() {
        return REDIS_SORTED_USER_FORMAT + userId;
    }

    @Override
    public String getUserType() {
        return "SORTED_SET_USER";
    }
}
