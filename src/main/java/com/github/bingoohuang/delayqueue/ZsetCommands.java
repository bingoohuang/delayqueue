package com.github.bingoohuang.delayqueue;

import java.util.Map;
import java.util.Set;

public interface ZsetCommands {
    Long zadd(String key, Map<String, Double> scoreMembers);

    Long zrem(String key, String... member);

    Set<String> zrangeByScore(String key, double min, double max, int offset, int count);
}
