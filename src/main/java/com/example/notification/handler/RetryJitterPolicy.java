package com.example.notification.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Encapsulates retry delay calculation with random jitter.
 *
 * Jitter prevents thundering-herd when many consumers fail at once
 * and all attempt to retry at the same scheduled moment.
 *
 * Formula:  actual_delay = base_delay + random(0, jitter_range)
 */
@Slf4j
@Component
public class RetryJitterPolicy {

    @Value("${kafka.retry.retry1-delay-ms:30000}")
    private long retry1DelayMs;

    @Value("${kafka.retry.retry1-jitter-ms:15000}")
    private long retry1JitterMs;

    @Value("${kafka.retry.retry2-delay-ms:300000}")
    private long retry2DelayMs;

    @Value("${kafka.retry.retry2-jitter-ms:60000}")
    private long retry2JitterMs;

    public long computeRetry1DelayMs() {
        long jitter = ThreadLocalRandom.current().nextLong(0, retry1JitterMs);
        long delay = retry1DelayMs + jitter;
        log.debug("Retry-1 delay={}ms (base={}ms jitter={}ms)", delay, retry1DelayMs, jitter);
        return delay;
    }

    public long computeRetry2DelayMs() {
        long jitter = ThreadLocalRandom.current().nextLong(0, retry2JitterMs);
        long delay = retry2DelayMs + jitter;
        log.debug("Retry-2 delay={}ms (base={}ms jitter={}ms)", delay, retry2DelayMs, jitter);
        return delay;
    }
}
