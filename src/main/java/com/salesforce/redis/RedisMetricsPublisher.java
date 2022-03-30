package com.salesforce.redis;

import redis.clients.jedis.Jedis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class RedisMetricsPublisher implements Runnable {
    @Override
    public void run() {
        String redisHost = System.getProperty("REDIS_HOST");
        String redisPort = System.getProperty("REDIS_PORT");
        Jedis jedis = new Jedis(URI.create("redis://" + redisHost+ ":" + redisPort));
        String auth = System.getProperty("AUTH");
        if(auth != null) {
            jedis.auth(auth);
        }
        String info = jedis.info();
        publishOpsPerSecMetric(info);
        publishCpuTimeMetric(info);
    }

    private void publishCpuTimeMetric(String info) {
        Pattern pattern = Pattern.compile(".*used_cpu_user:(\\d+).*");
        Matcher matcher = pattern.matcher(info);
        if (matcher.find()) {
            String cpuTime = matcher.group(1);
            try (BufferedWriter writer = new BufferedWriter(
                    new FileWriter("/Users/sperumalla/Documents/SF/repos/redisloadclient/cpu_time.txt",
                            true));) {
                writer.append(cpuTime);
                writer.append("\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void publishOpsPerSecMetric(String info) {
        Pattern pattern = Pattern.compile(".*instantaneous_ops_per_sec:(\\d+).*");
        Matcher matcher = pattern.matcher(info);
        if (matcher.find()) {
            String opsPerSec = matcher.group(1);
            String ops_file_path = System.getProperty("OPS_FILE");
            try (BufferedWriter writer = new BufferedWriter(
                    new FileWriter(ops_file_path,
                            true));) {
                writer.append(opsPerSec);
                writer.append("\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
