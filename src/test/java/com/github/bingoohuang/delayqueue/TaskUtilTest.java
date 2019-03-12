package com.github.bingoohuang.delayqueue;

import com.github.bingoohuang.utils.lang.Threadx;
import org.joda.time.DateTime;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class TaskUtilTest {

  @Test
  public void randomSleep() {
    Thread.currentThread().interrupt();
    Threadx.randomSleepMillis(10, 20);
  }

  @Test
  public void emptyThenNow() {
    assertThat(TaskUtil.emptyThenNow(null, null)).isNotNull();
    assertThat(TaskUtil.emptyThenNow(DateTime.now(), null)).isNotNull();
  }
}
