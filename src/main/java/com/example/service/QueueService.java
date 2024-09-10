package com.example.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;

@Service
public class QueueService {

    private final RedisTemplate<String, Object> redisTemplate;

    @Autowired
    public QueueService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void incrementQueueSize(String serviceName) {
        String key = getQueueKey(serviceName);
        redisTemplate.opsForValue().increment(key);
    }

    public void decrementQueueSize(String serviceName) {
        String key = getQueueKey(serviceName);
        redisTemplate.opsForValue().decrement(key);
    }

    public Long getQueueSize(String serviceName) {
        String key = getQueueKey(serviceName);
        Object size = redisTemplate.opsForValue().get(key);
        return size == null ? 0 : (Long) size;
    }

    public String getLeastBusyService(List<String> services) {
        return services.stream()
                .min(Comparator.comparingLong(this::getQueueSize))  // Compare based on queue size
                .orElse(null);
    }

    private String getQueueKey(String serviceName) {
        return "queue:" + serviceName;
    }
}