package com.salesforce.redis;

import java.util.List;

public interface RedisUser extends Runnable {
    long getNoOfOps();

    List<Long> getLatencies();

    String getName();

    String getUserType();
}
