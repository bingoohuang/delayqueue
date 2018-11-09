package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.utils.cron.CronExpression;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.n3r.eql.util.Pair;

import java.util.concurrent.*;

@Slf4j
public class TaskUtil {
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

    public static DateTime emptyThenNow(DateTime dateTime, CronExpression cron) {
        return dateTime != null
                ? dateTime

                : cron == null
                ? DateTime.now()

                : cron.nextTimeAfter(DateTime.now());
    }

    /**
     * 获取Spring Bean的默认名字。参考org.springframework.context.annotation.AnnotationBeanNameGenerator文档。
     * Derive a default bean name from the given bean definition.
     * The default implementation simply builds a decapitalized version of the short class name: e.g. "mypackage.MyJdbcDao" - "myJdbcDao".
     * <p>
     * Note that inner classes will thus have names of the form "outerClassName.InnerClassName",
     * which because of the period in the name may be an issue if you are autowiring by name.
     *
     * @param beanClass spring bean class
     * @return spring bean name
     */
    public static String getSpringBeanDefaultName(Class<?> beanClass) {
        Class<?> enclosingClass = beanClass.getEnclosingClass();
        if (enclosingClass == null) return StringUtils.uncapitalize(beanClass.getSimpleName());

        return getSpringBeanDefaultName(enclosingClass) + "." + beanClass.getSimpleName();
    }
}
