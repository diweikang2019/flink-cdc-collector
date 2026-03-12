package com.zhonghe.flink.cdc.collector;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: diweikang
 * @Date: 2026/3/9 17:05
 * @Description:
 */
public class BusinessKeyExtractor {

    private static final Logger log = LoggerFactory.getLogger(BusinessKeyExtractor.class);

    /**
     * 从Debezium消息中提取业务ID
     *
     * @param tableName 表名
     * @param value     Debezium消息JSON
     * @return 业务ID（带表名前缀）
     */
    public static String extractBusinessKey(String tableName, String value) {
        try {
            JSONObject json = JSONObject.parseObject(value);

            // 优先从after中取，如果没有则从before中取（DELETE操作）
            JSONObject data = json.getJSONObject("after");
            if (data == null) {
                data = json.getJSONObject("before");
            }

            if (data == null) {
                log.warn("无法提取业务ID: 无数据内容, table={}", tableName);
                return "unknown-" + System.currentTimeMillis();
            }

            String businessId = null;

            // 根据表名提取不同的主键
            switch (tableName) {
                case "crm_student":
                    businessId = "student_" + data.getString("id");
                    break;

                case "crm_clue_extend":
                    // 优先使用clue_id，如果没有则用id
                    String clueId = data.getString("clue_id");
                    if (clueId != null && !clueId.isEmpty()) {
                        businessId = "clue_" + clueId;
                    } else {
                        businessId = "clue_" + data.getString("id");
                    }
                    break;

                default:
                    // 默认使用id字段
                    businessId = tableName + "_" + data.getString("id");
            }

            log.debug("提取业务ID: table={}, businessId={}", tableName, businessId);
            return businessId;

        } catch (Exception e) {
            log.error("提取业务ID失败: table={}, value={}", tableName, value, e);
            return "error-" + System.currentTimeMillis();
        }
    }

    /**
     * 从SourceRecord中获取表名
     *
     * @param topic topic名称 (格式: mysql_binlog_source.database.table)
     * @return 表名
     */
    public static String extractTableName(String topic) {
        String[] parts = topic.split("\\.");
        if (parts.length >= 3) {
            return parts[2];  // mysql_binlog_source.test_crm_db.crm_student -> crm_student
        }
        return "unknown";
    }
}
