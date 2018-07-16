CREATE TABLE t_delay_task (
 TASK_ID varchar(20) NOT NULL COMMENT '任务ID',
 RELATIVE_ID varchar(20) NULL COMMENT '关联ID',
 TASK_NAME varchar(100) NOT NULL COMMENT '任务名称',
 TASK_SERVICE varchar(100) NOT NULL COMMENT '任务服务名称，需要实现Taskable接口',
 STATE varchar(3) NOT NULL COMMENT '待运行/运行中/已成功/已失败/已取消',
 READY_TIME datetime NOT NULL COMMENT '可以开始运行的时间，此参数可以设置延时',
 TIMEOUT tinyint NOT NULL DEFAULT 0 COMMENT '超时时间（秒）',
 START_TIME datetime NULL COMMENT '开始运行时间',
 END_TIME datetime NULL COMMENT '结束运行时间',
 RESULT varchar(300) NULL COMMENT '任务运行结果',
 ATTACHMENT   TEXT  NULL COMMENT '附件',
 UPDATE_TIME datetime NOT NULL  COMMENT '更新时间',
 CREATE_TIME datetime NOT NULL  COMMENT '创建时间',
 PRIMARY KEY (TASK_ID)
)  COMMENT = '任务表';

