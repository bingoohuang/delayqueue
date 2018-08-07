package com.github.bingoohuang.delayqueue;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import org.joda.time.DateTime;
import org.n3r.eql.util.Pair;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.*;

@Slf4j
public class TaskUtil {
    private final static ThreadLocalRandom random = ThreadLocalRandom.current();

    /**
     * Sleep in random time between minMillis and maxMillis.
     *
     * @param minMillis minimum time in millis
     * @param maxMillis maximum time in millis
     * @return true if interrupted
     */
    public static boolean randomSleepMillis(int minMillis, int maxMillis) {
        try {
            Thread.sleep(random.nextInt(maxMillis - minMillis) + minMillis);
            return false;
        } catch (InterruptedException e) {
            return true;
        }
    }

    @SneakyThrows
    public static <T> Pair<? extends T, Boolean> timeoutRun(ExecutorService executorService, Callable<? extends T> runnable, int timeout) {
        if (timeout <= 0) return Pair.of(runnable.call(), false);

        val service = executorService != null ? executorService : Executors.newSingleThreadExecutor();
        val future = service.submit(runnable);
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

    @SuppressWarnings("unchecked")
    public static <T> T adapt(Object obj, Class<? extends T> target) {
        return (T) (target.isAssignableFrom(obj.getClass())
                ? obj
                : target.isInterface()

                ? Proxy.newProxyInstance(target.getClassLoader(), new Class[]{target}, (p, m, args) -> adapt(obj, m, args))
                : Enhancer.create(target, new Class[]{}, (MethodInterceptor) (o, m, args, p) -> adapt(obj, m, args)));
    }

    @SneakyThrows
    public static Object adapt(Object obj, Method adapted, Object[] args) {
        return findAdapted(obj, adapted).invoke(obj, args);
    }

    @SneakyThrows
    public static Method findAdapted(Object obj, Method adapted) {
        return obj.getClass().getMethod(adapted.getName(), adapted.getParameterTypes());
    }
}
