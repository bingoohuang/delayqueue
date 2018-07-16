package com.github.bingoohuang.delayqueue;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

@Data @AllArgsConstructor @NoArgsConstructor @Builder
public class TaskItem {
    private String taskId;
    private String relativeId;
    private String taskName;
    private String taskService;
    private String state;
    private DateTime readyTime;
    private int timeout;
    private DateTime startTime;
    private DateTime endTime;
    private String result;
    private String attachment;
    private DateTime updateTime;
    private DateTime createTime;

    public static final String 待运行 = "待运行";
    public static final String 运行中 = "运行中";
    public static final String 已完成 = "已完成";
    public static final String 已失败 = "已失败";
    public static final String 已取消 = "已取消";
    public static final String 已超时 = "已超时";
}
