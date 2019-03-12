package com.github.bingoohuang.delayqueue;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AttachmentVo {
  private String name;
  private int age;
  private DateTime createTime;
}
