package com.github.bingoohuang.delayqueue;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.joda.time.DateTime;

import java.util.concurrent.*;

@Slf4j
public class Util {

    private final static ThreadLocalRandom random = ThreadLocalRandom.current();

    public static void randomSleep(int min, int max, TimeUnit timeUnit) {
        try {
            int timeout = random.nextInt(max - min) + min;
            timeUnit.sleep(timeout);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @SneakyThrows
    public static boolean timeoutRun(Runnable runnable, int timeout) {
        if (timeout <= 0 ) {
            runnable.run();
            return false;
        }

        val executorService = Executors.newSingleThreadExecutor();

        val future = executorService.submit(runnable);
        try {
            future.get(timeout, TimeUnit.SECONDS);
            return false;
        } catch (TimeoutException e) {
            log.warn("任务超时了，超时{}秒", timeout);
            return true;
        } catch (InterruptedException | ExecutionException e) {
            throw e.getCause();
        }
    }

    public static DateTime emptyThenNow(DateTime dateTime) {
        return dateTime == null ? DateTime.now() : dateTime;
    }
}
