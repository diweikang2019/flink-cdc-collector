package com.zhonghe.flink.cdc.collector;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * @Author: diweikang
 * @Date: 2026/3/9 17:05
 * @Description:
 */
public class BusinessKeyExtractor {

    private static final Logger log = LoggerFactory.getLogger(BusinessKeyExtractor.class);

    /**
     * 生成确定性的 fallback key（用于保证重启重放下路由/幂等key稳定，避免用当前时间戳）。
     * <p>
     * 这里优先使用 Debezium envelope 中的：
     * - 顶层/来源时间戳：ts_ms
     * - binlog 文件偏移：source.file + source.pos（可选 row）
     */
    private static String buildFallbackKey(JSONObject json, JSONObject source, String value) {
        try {
            long tsMs = json.getLongValue("ts_ms");
            if (tsMs == 0 && source != null) {
                tsMs = source.getLongValue("ts_ms");
            }

            String file = source == null ? null : source.getString("file");
            Object posObj = source == null ? null : source.get("pos");
            Object rowObj = source == null ? null : source.get("row");
            String op = json.getString("op");

            // 若关键信息都缺失，退化到内容 hash（同样是确定性的）。
            boolean hasAnyCdcPos = (file != null && !file.isEmpty()) || posObj != null || rowObj != null || tsMs != 0;
            if (!hasAnyCdcPos) {
                return "unknown-" + sha256Prefix(value);
            }

            // file 本身就是 String，无需 String.valueOf；其余字段允许为 null，用 String.valueOf 保持 "null" 的确定性输出
            return "unknown-" + tsMs + "-" + file + "-" + posObj + "-" + rowObj + "-" + op;
        } catch (Exception e) {
            return "error-" + sha256Prefix(value);
        }
    }

    /**
     * 对输入内容生成一个“短且稳定”的指纹串，用于 fallback key 的确定性部分。
     * <p>
     * 设计目的：
     * - 同一条 CDC 原始 JSON（value）在失败重启/重放时必须得到相同的 key，避免用时间戳导致 key 漂移
     * - 当缺失 {@code ts_ms}/{@code source.file}/{@code source.pos} 等定位信息时，仍能生成确定性 key
     * <p>
     * 实现说明：
     * - 使用 SHA-256 摘要的前 8 个字节（16 个 hex 字符），仅用于标识/路由，不承担加密安全目的
     * - 若 SHA-256 不可用（理论上极少发生），退化为 {@code String.hashCode()}（仍避免使用当前时间戳）
     */
    private static String sha256Prefix(String input) {
        try {
            if (input == null) {
                input = "null";
            }
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            // 前 16 hex 字符足够用于确定性标识（不用于加密安全）
            StringBuilder sb = new StringBuilder(16);
            for (int i = 0; i < 8; i++) {
                sb.append(String.format("%02x", hash[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            // 极端情况下退化为 Java 的确定性 hash（同一输入在同一次 JVM 生命周期里稳定；这里仍优先避免时间戳）
            return String.valueOf(input.hashCode());
        }
    }

    /**
     * 从 Debezium 消息中提取“业务 Key”（用于 Kafka record key）。
     * <p>
     * 设计目标：
     * - **同一业务实体的多次变更落到同一 key**：从而在 Kafka 分区内保持相对有序，便于下游按 key 聚合/去重/幂等处理
     * - **跨表归一**：部分“明细/关联表”不使用自身主键，而是映射到所属主实体的 key（例如跟随、线索扩展归一到 student）
     * - **容错确定性**：当解析失败或缺失 after/before 时，仍返回确定性的 fallback key，避免重启重放导致 key 漂移
     *
     * @param tableName 表名
     * @param value     Debezium消息JSON
     * @return 业务ID（带表名前缀）
     */
    public static String extractBusinessKey(String tableName, String value) {
        try {
            JSONObject json = JSONObject.parseObject(value);
            JSONObject source = json.getJSONObject("source");

            // 优先从after中取，如果没有则从before中取（DELETE操作）
            JSONObject data = json.getJSONObject("after");
            if (data == null) {
                data = json.getJSONObject("before");
            }

            if (data == null) {
                log.warn("无法提取业务ID: 无数据内容, table={}", tableName);
                return buildFallbackKey(json, source, value);
            }

            String businessId;
            String safeTableName = (tableName == null || tableName.isEmpty()) ? "unknown_table" : tableName;

            // 按表名确定 key 规则：
            // - 主表用自身主键（例如 crm_student -> student_id）
            // - 明细/关联表尽量映射到“所属主实体”的 key（让同一实体相关的变更聚到同一 Kafka key）
            switch (safeTableName) {
                case "crm_student":
                    businessId = "student_" + data.getString("id");
                    break;
                case "crm_clue_extend":
                    // clue_extend 归一到 student（clue_id 对应学生）
                    businessId = "student_" + data.getString("clue_id");
                    break;
                case "crm_follow":
                    // follow 归一到 student（student_id 对应学生）
                    businessId = "student_" + data.getString("student_id");
                    break;
                case "crm_qw_retailcode_user":
                    businessId = "qw_user_" + data.getString("id");
                    break;
                case "crm_qw_friend_info":
                    businessId = "qw_friend_info_" + data.getString("id");
                    break;
                case "crm_qw_friend_mobiles":
                    businessId = "qw_friend_info_" + data.getString("qw_friend_info_id");
                    break;
                case "crm_order":
                    businessId = "order_" + data.getString("trade_no");
                    break;
                case "crm_service_details":
                    // service_details 归一到 serve_record（serve_record_id 对应主单）
                    businessId = "serve_record_" + data.getString("serve_record_id");
                    break;
                case "serve_record":
                    businessId = "serve_record_" + data.getString("id");
                    break;
                case "crm_qw_groupchat":
                    businessId = "groupchat_" + data.getString("id");
                    break;
                case "crm_qw_groupchat_tag":
                    // groupchat_tag 归一到 groupchat（groupchat_id 对应群聊）
                    businessId = "groupchat_" + data.getString("groupchat_id");
                    break;
                case "crm_admin":
                    businessId = "admin_" + data.getString("id");
                    break;
                default:
                    // 默认使用id字段
                    businessId = safeTableName + "_" + data.getString("id");
            }

            log.debug("提取业务ID: table={}, businessId={}", tableName, businessId);
            return businessId;

        } catch (Exception e) {
            log.error("提取业务ID失败: table={}, value={}", tableName, value, e);
            return "error-" + sha256Prefix(value);
        }
    }
}
