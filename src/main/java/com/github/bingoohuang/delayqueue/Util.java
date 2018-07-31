package com.github.bingoohuang.delayqueue;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.joda.time.DateTime;
import org.n3r.eql.util.Pair;

import java.util.concurrent.*;

@Slf4j
public class Util {
    private final static ThreadLocalRandom random = ThreadLocalRandom.current();

    /**
     * Sleep in random time between min and max.
     *
     * @param min      minimum time
     * @param max      maximum time
     * @param timeUnit time unit
     * @return true if interrupted
     */
    public static boolean randomSleep(int min, int max, TimeUnit timeUnit) {
        try {
            timeUnit.sleep(random.nextInt(max - min) + min);
            return false;
        } catch (InterruptedException e) {
            return true;
        }
    }

    @SneakyThrows
    public static <T> Pair<T, Boolean> timeoutRun(Callable<T> runnable, int timeout) {
        if (timeout <= 0) return Pair.of(runnable.call(), false);

        val executorService = Executors.newSingleThreadExecutor();
        val future = executorService.submit(runnable);
        try {
            return Pair.of(future.get(timeout, TimeUnit.SECONDS), false);
        } catch (TimeoutException e) {
            log.warn("任务超时了，超时{}秒", timeout);
            return Pair.of(null, true);
        } catch (InterruptedException | ExecutionException e) {
            throw e.getCause();
        }
    }

    public static DateTime emptyThenNow(DateTime dateTime) {
        return dateTime == null ? DateTime.now() : dateTime;
    }
}
