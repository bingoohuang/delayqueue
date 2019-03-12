package com.github.bingoohuang.delayqueue;

import com.alibaba.fastjson.annotation.JSONField;
import com.github.bingoohuang.westcache.utils.FastJsons;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TaskItem {
  private String taskId; // 任务ID
  private String relativeId; // 关联ID(比如订单ID)，比如订单ID\会员卡ID\排期ID等
  private String classifier; // 任务分类
  private String taskName; // 任务名称
  private String taskService; // 任务服务名称，需要实现Taskable接口
  private String state; // 待运行/运行中/已成功/已失败/已取消/已超时
  private DateTime runAt; // 何时运行，此参数可以设置延时
  private int timeout; // 超时（秒）
  private DateTime startTime; // 开始运行时间
  private DateTime endTime; // 结束运行时间
  private String resultState; // 任务执行状态
  private String resultStore; // 结果存储方式，DIRECT直接存储在RESULT字段中，REDIS存储在REDIS中
  private String result; // 任务运行结果
  private String hostname; // 客户端主机名字
  private String clientIp; // 客户端IP
  private String attachment; // 附件
  private String var1; // 参数1
  private String var2; // 参数2
  private String var3; // 参数3
  private String scheduled; // 排期
  private DateTime createTime; // 创建时间
  private long versionNumber; // 任务创建时的程序版本号

  private boolean invokeTimeout; // 是否调用超时

  public static final String 待运行 = "待运行";
  public static final String 运行中 = "运行中";
  public static final String 已完成 = "已完成";
  public static final String 已失败 = "已失败";
  public static final String 已取消 = "已取消";
  public static final String 已超时 = "已超时";

  @JSONField(serialize = false)
  public String getAttachmentAsString() {
    return getAttachment(String.class);
  }

  public <T> T getAttachment(Class<T> clazz) {
    return FastJsons.parse(attachment, clazz);
  }

  @JSONField(serialize = false)
  public String getResultAsString() {
    return getResult(String.class);
  }

  public <T> T getResult(Class<T> clazz) {
    return FastJsons.parse(result, clazz);
  }

  @JSONField(serialize = false)
  public boolean isComplete() {
    return 已完成.equals(state);
  }

  @JSONField(serialize = false)
  public boolean isReadyRun() {
    return 待运行.equals(state);
  }
}
