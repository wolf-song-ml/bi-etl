package com.z.etl.common;

/**
 * 全局常量类
 * 
 */
public class GlobalConstants {
    /**
     * 一天的毫秒数
     */
    public static final int DAY_OF_MILLISECONDS = 86400000;
    /**
     * 存储在HDFS路径上的日志文件前缀
     */
    public static final String HDFS_LOGS_PATH_PREFIX = "/event_logs";
    /**
     * 定义的运行时间变量名
     */
    public static final String RUNNING_DATE_PARAMES = "RUNNING_DATE";
    /**
     * 定义的运行时etl操作是否覆盖hbase表，如果为参数值为true，表示覆盖，那么表示重新创建，否则不进行重新创建
     */
    public static final String RUNNING_OVERRIDE_ETL_HBASE_TABLE = "";
    /**
     * 默认值
     */
    public static final String DEFAULT_VALUE = "unknown";
    /**
     * 维度信息表中指定全部列值
     */
    public static final String VALUE_OF_ALL = "all";

    /**
     * 定义的output collector的前缀
     */
    public static final String OUTPUT_COLLECTOR_KEY_PREFIX = "collector_";

    /**
     * 指定连接表配置为report
     */
    public static final String WAREHOUSE_OF_REPORT = "report";

    /**
     * 批量执行的key
     */
    public static final String JDBC_BATCH_NUMBER = "mysql.batch.number";

    /**
     * 默认批量大小
     */
    public static final String DEFAULT_JDBC_BATCH_NUMBER = "500";

    /**
     * driver 名称
     */
    public static final String JDBC_DRIVER = "mysql.%s.driver";

    /**
     * JDBC URL
     */
    public static final String JDBC_URL = "mysql.%s.url";

    /**
     * username名称
     */
    public static final String JDBC_USERNAME = "mysql.%s.username";

    /**
     * password名称
     */
    public static final String JDBC_PASSWORD = "mysql.%s.password";

}
