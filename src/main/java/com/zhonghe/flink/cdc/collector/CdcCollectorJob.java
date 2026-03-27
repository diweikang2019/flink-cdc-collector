package com.zhonghe.flink.cdc.collector;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 完整配置，支持事物，支持EXACTLY_ONCE
 *
 * @Author: diweikang
 * @Date: 2026/3/9 17:06
 * @Description:
 */
public class CdcCollectorJob {

    private static final Logger log = LoggerFactory.getLogger(CdcCollectorJob.class);

    public static void main(String[] args) throws Exception {
        String profile = resolveProfile();
        JobConfig config = loadConfig(profile);

        log.info("启动CRM CDC采集作业...");
        log.info("当前环境配置 profile={}", profile);
        log.info("MySQL: {}:{}, database={}", config.mysqlHost, config.mysqlPort, config.mysqlDatabase);
        log.info("Kafka: {}, topic={}", config.kafkaBootstrapServers, config.kafkaTopic);
        log.info("并行度: {}, server-id范围: {}", config.parallelism, config.serverIdRange);

        // 1. 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.1 开启Checkpoint，设置EXACTLY_ONCE模式
        env.enableCheckpointing(config.checkpointIntervalMs);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(config.checkpointTimeoutMs);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.minPauseBetweenCheckpointsMs);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(config.maxConcurrentCheckpoints);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 1.2 重启策略：避免临时抖动直接失败退出
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(config.restartAttempts, Time.seconds(config.restartDelaySeconds)));

        // 2 设置并行度
        env.setParallelism(config.parallelism);

        // 3. 创建MySQL CDC Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(config.mysqlHost)
                .port(config.mysqlPort)
                .databaseList(config.mysqlDatabase)
                .tableList(config.mysqlTables)
                .username(config.mysqlUsername)
                .password(config.mysqlPassword)
                .deserializer(new BusinessJsonDebeziumDeserializationSchema()) // 转成下游可直接使用的业务JSON
                .serverId(config.serverIdRange)
                .startupOptions(StartupOptions.latest()) // 或initial()
                .build();

        // 4. 添加Source
        DataStreamSource<String> cdcStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "mysql-cdc-source"
        );

        // 5. 原始数据打印（调试用；生产建议关闭或降频）
        cdcStream.map(json -> {
            log.info("【调试】收到原始数据: {}", json);
            return json;
        }).name("debug-print-raw-cdc").print();

        // 6. 处理流：提取业务ID用于Kafka分区（但不修改数据内容）
        DataStream<KafkaMessage> kafkaStream = cdcStream.process(new ProcessFunction<String, KafkaMessage>() {

            @Override
            public void processElement(String value, Context ctx, Collector<KafkaMessage> out) {
                try {
                    // 解析JSON获取元数据
                    JSONObject json = JSONObject.parseObject(value);
                    JSONObject source = json.getJSONObject("source");
                    if (source == null) {
                        log.warn("消息缺少source字段: {}", value);
                    }

                    // 从source中获取表名（source 缺失时 table 可能为 null）
                    String table = source == null ? null : source.getString("table");

                    // 提取业务ID
                    String businessKey = BusinessKeyExtractor.extractBusinessKey(table, value);

                    // 输出到下游
                    out.collect(new KafkaMessage(businessKey, value));

                    log.info("处理消息: key={}, table={}", businessKey, table);

                } catch (Exception e) {
                    log.error("处理消息失败: {}", value, e);
                    // 保证每个输入事件都能产生确定性的输出（避免“业务层丢消息”）
                    String businessKey = BusinessKeyExtractor.extractBusinessKey(null, value);
                    out.collect(new KafkaMessage(businessKey, value));
                }
            }
        }).name("extract-business-key");

        // 7. Kafka生产者配置（关键：保证发送顺序和Exactly-Once配置）
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers);

        // Kafka生产者Exactly-Once配置
        kafkaProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");      // 幂等性
        kafkaProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");                     // 所有副本确认
        kafkaProps.setProperty(ProducerConfig.RETRIES_CONFIG, "5");                    // 重试次数
        kafkaProps.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1"); // 关键：防止重试乱序

        // 事务配置（必须与broker匹配）
        kafkaProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(config.transactionTimeoutMs));

        // 8. 创建Kafka Sink
        final String kafkaTopic = config.kafkaTopic;
        KafkaSink<KafkaMessage> kafkaSink = KafkaSink.<KafkaMessage>builder()
                .setBootstrapServers(config.kafkaBootstrapServers)
                // Flink EOS 事务前缀：让 Flink 负责为每个 subtask 生成唯一 transactionalId，避免手工配置冲突
                .setTransactionalIdPrefix("cdc-transaction")
                .setRecordSerializer((KafkaRecordSerializationSchema<KafkaMessage>) (message, context, timestamp) -> new ProducerRecord<>(
                        kafkaTopic,
                        message.key.getBytes(StandardCharsets.UTF_8),      // 业务ID作为key
                        message.value.getBytes(StandardCharsets.UTF_8)     // 原始CDC数据作为value
                ))
                .setKafkaProducerConfig(kafkaProps)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        // 9. 添加Sink
        kafkaStream
                .sinkTo(kafkaSink)
                .name("kafka-sink-crm-cdc")
                .uid("kafka-sink-crm-cdc");

        // 10. 发送监控日志（显式接 print sink，确保该分支会执行）
        kafkaStream
                .map(message -> {
                    log.info("准备发送Kafka: key={}, size={} bytes", message.key, message.value.length());
                    JSONObject debugJson = new JSONObject();
                    debugJson.put("key", message.key);
                    try {
                        debugJson.put("value", JSONObject.parseObject(message.value));
                    } catch (Exception e) {
                        // value 不是标准 JSON 时，降级为原始字符串输出
                        debugJson.put("value", message.value);
                    }
                    return debugJson.toJSONString();
                })
                .name("debug-print-kafka-message")
                .print();

        // 11. 执行作业
        log.info("启动Flink作业...");
        env.execute("CRM-CDC-Collector");
    }

    /**
     * 内部类：封装Kafka消息
     */
    public static class KafkaMessage {
        public final String key;      // 业务ID，用于分区
        public final String value;    // 原始CDC数据

        public KafkaMessage(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * 将 Debezium SourceRecord 转成“下游可直接消费”的业务 JSON：
     * - 保留 before/after/source/op/ts_ms
     * - 提升 database/table 到顶层
     * - Decimal 逻辑类型统一转为可读数字字符串，避免 base64 形态
     */
    public static class BusinessJsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(SourceRecord record, Collector<String> out) {
            Object valueObj = record.value();
            if (!(valueObj instanceof Struct)) {
                out.collect(String.valueOf(valueObj));
                return;
            }

            Struct value = (Struct) valueObj;
            Struct source = value.getStruct("source");
            Struct before = value.getStruct("before");
            Struct after = value.getStruct("after");

            JSONObject event = new JSONObject();
            event.put("op", extractOperation(record, value));
            event.put("ts_ms", value.getInt64("ts_ms"));
            event.put("source", source == null ? null : (JSONObject) convertValue(source, source.schema()));
            event.put("before", before == null ? null : (JSONObject) convertValue(before, before.schema()));
            event.put("after", after == null ? null : (JSONObject) convertValue(after, after.schema()));

            if (source != null) {
                event.put("database", source.getString("db"));
                event.put("table", source.getString("table"));
            }

            out.collect(event.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }

        @SuppressWarnings("unchecked")
        private Object convertValue(Object value, Schema schema) {
            if (value == null) {
                return null;
            }
            if (schema == null) {
                return value;
            }

            if (Decimal.LOGICAL_NAME.equals(schema.name())) {
                BigDecimal decimal = toBigDecimal(value, schema);
                return decimal == null ? null : decimal.toPlainString();
            }

            switch (schema.type()) {
                case STRUCT:
                    Struct struct = (Struct) value;
                    JSONObject obj = new JSONObject();
                    for (Field field : schema.fields()) {
                        Object fieldValue = struct.get(field);
                        obj.put(field.name(), convertValue(fieldValue, field.schema()));
                    }
                    return obj;
                case ARRAY:
                    JSONArray arr = new JSONArray();
                    List<Object> list = (List<Object>) value;
                    for (Object item : list) {
                        arr.add(convertValue(item, schema.valueSchema()));
                    }
                    return arr;
                case MAP:
                    JSONObject mapObj = new JSONObject();
                    Map<Object, Object> map = (Map<Object, Object>) value;
                    for (Map.Entry<Object, Object> entry : map.entrySet()) {
                        String key = String.valueOf(entry.getKey());
                        mapObj.put(key, convertValue(entry.getValue(), schema.valueSchema()));
                    }
                    return mapObj;
                case BYTES:
                    byte[] bytes = (byte[]) value;
                    return Base64.getEncoder().encodeToString(bytes);
                default:
                    return value;
            }
        }

        private BigDecimal toBigDecimal(Object value, Schema schema) {
            if (value == null) {
                return null;
            }
            if (value instanceof BigDecimal) {
                return (BigDecimal) value;
            }
            if (value instanceof byte[]) {
                return Decimal.toLogical(schema, (byte[]) value);
            }
            return new BigDecimal(value.toString());
        }

        private String extractOperation(SourceRecord record, Struct value) {
            try {
                return Envelope.operationFor(record).code();
            } catch (Exception e) {
                Object op = value.get("op");
                return op == null ? "u" : String.valueOf(op);
            }
        }
    }

    private static String resolveProfile() {
        String profile = System.getProperty("profile");
        if (profile == null || profile.trim().isEmpty()) {
            profile = System.getenv("APP_PROFILE");
        }
        if (profile == null || profile.trim().isEmpty()) {
            profile = "test";
        }
        return profile.trim().toLowerCase();
    }

    private static JobConfig loadConfig(String profile) {
        String resourceName = "application-" + profile + ".yml";
        try (InputStream in = CdcCollectorJob.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IllegalArgumentException("未找到配置文件: " + resourceName);
            }
            Map<String, String> kv = parseSimpleYaml(in);
            return JobConfig.from(kv, resourceName);
        } catch (IOException e) {
            throw new RuntimeException("读取配置文件失败: " + resourceName, e);
        }
    }

    /**
     * 轻量 YAML 解析器：
     * - 支持基于缩进的分层 key（会展平为 a.b.c）
     * - 支持列表项 "- value"（会拼成逗号分隔字符串）
     * - 仅覆盖当前项目配置使用的语法子集
     */
    private static Map<String, String> parseSimpleYaml(InputStream in) throws IOException {
        Map<String, String> result = new HashMap<>();
        Deque<Integer> indentStack = new ArrayDeque<>();
        Deque<String> pathStack = new ArrayDeque<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                int indent = countLeadingSpaces(line);
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }

                while (!indentStack.isEmpty() && indent <= indentStack.peek()) {
                    indentStack.pop();
                    pathStack.pop();
                }

                if (trimmed.startsWith("- ")) {
                    if (pathStack.isEmpty()) {
                        continue;
                    }
                    String listKey = pathStack.peek();
                    String item = stripYamlQuotes(trimmed.substring(2).trim());
                    result.compute(listKey, (k, oldValue) -> oldValue == null || oldValue.isEmpty() ? item : oldValue + "," + item);
                    continue;
                }

                int idx = trimmed.indexOf(':');
                if (idx <= 0) {
                    continue;
                }
                String key = trimmed.substring(0, idx).trim();
                String rawValue = trimmed.substring(idx + 1).trim();
                String parent = pathStack.isEmpty() ? "" : pathStack.peek();
                String fullKey = parent.isEmpty() ? key : parent + "." + key;

                if (rawValue.isEmpty()) {
                    indentStack.push(indent);
                    pathStack.push(fullKey);
                } else {
                    String value = stripYamlQuotes(rawValue);
                    result.put(fullKey, value);
                }
            }
        }
        return result;
    }

    private static int countLeadingSpaces(String line) {
        int count = 0;
        while (count < line.length() && line.charAt(count) == ' ') {
            count++;
        }
        return count;
    }

    private static String stripYamlQuotes(String value) {
        if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static String require(Map<String, String> kv, String key, String source) {
        String value = kv.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("配置缺失: " + key + " in " + source);
        }
        return value.trim();
    }

    private static class JobConfig {
        final String mysqlHost;
        final int mysqlPort;
        final String mysqlUsername;
        final String mysqlPassword;
        final String mysqlDatabase;
        final String[] mysqlTables;
        final String kafkaBootstrapServers;
        final String kafkaTopic;
        final int parallelism;
        final String serverIdRange;
        final long checkpointIntervalMs;
        final long checkpointTimeoutMs;
        final long minPauseBetweenCheckpointsMs;
        final int maxConcurrentCheckpoints;
        final int restartAttempts;
        final int restartDelaySeconds;
        final long transactionTimeoutMs;

        private JobConfig(String mysqlHost, int mysqlPort, String mysqlUsername, String mysqlPassword,
                          String mysqlDatabase, String[] mysqlTables, String kafkaBootstrapServers, String kafkaTopic,
                          int parallelism, String serverIdRange, long checkpointIntervalMs, long checkpointTimeoutMs,
                          long minPauseBetweenCheckpointsMs, int maxConcurrentCheckpoints, int restartAttempts,
                          int restartDelaySeconds, long transactionTimeoutMs) {
            this.mysqlHost = mysqlHost;
            this.mysqlPort = mysqlPort;
            this.mysqlUsername = mysqlUsername;
            this.mysqlPassword = mysqlPassword;
            this.mysqlDatabase = mysqlDatabase;
            this.mysqlTables = mysqlTables;
            this.kafkaBootstrapServers = kafkaBootstrapServers;
            this.kafkaTopic = kafkaTopic;
            this.parallelism = parallelism;
            this.serverIdRange = serverIdRange;
            this.checkpointIntervalMs = checkpointIntervalMs;
            this.checkpointTimeoutMs = checkpointTimeoutMs;
            this.minPauseBetweenCheckpointsMs = minPauseBetweenCheckpointsMs;
            this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
            this.restartAttempts = restartAttempts;
            this.restartDelaySeconds = restartDelaySeconds;
            this.transactionTimeoutMs = transactionTimeoutMs;
        }

        static JobConfig from(Map<String, String> kv, String source) {
            String mysqlHost = require(kv, "mysql.host", source);
            int mysqlPort = Integer.parseInt(require(kv, "mysql.port", source));
            String mysqlUsername = require(kv, "mysql.username", source);
            String mysqlPassword = require(kv, "mysql.password", source);
            String mysqlDatabase = require(kv, "mysql.database", source);
            String[] mysqlTables = require(kv, "mysql.tables", source).split("\\s*,\\s*");
            String kafkaBootstrapServers = require(kv, "kafka.bootstrap.servers", source);
            String kafkaTopic = require(kv, "kafka.topic", source);
            int parallelism = Integer.parseInt(require(kv, "flink.parallelism", source));
            String serverIdRange = require(kv, "mysql.server-id-range", source);
            long checkpointIntervalMs = Long.parseLong(require(kv, "flink.checkpoint.interval-ms", source));
            long checkpointTimeoutMs = Long.parseLong(require(kv, "flink.checkpoint.timeout-ms", source));
            long minPauseBetweenCheckpointsMs = Long.parseLong(require(kv, "flink.checkpoint.min-pause-ms", source));
            int maxConcurrentCheckpoints = Integer.parseInt(require(kv, "flink.checkpoint.max-concurrent", source));
            int restartAttempts = Integer.parseInt(require(kv, "flink.restart.attempts", source));
            int restartDelaySeconds = Integer.parseInt(require(kv, "flink.restart.delay-seconds", source));
            long transactionTimeoutMs = Long.parseLong(require(kv, "kafka.transaction.timeout-ms", source));
            return new JobConfig(mysqlHost, mysqlPort, mysqlUsername, mysqlPassword, mysqlDatabase, mysqlTables,
                    kafkaBootstrapServers, kafkaTopic, parallelism, serverIdRange, checkpointIntervalMs,
                    checkpointTimeoutMs, minPauseBetweenCheckpointsMs, maxConcurrentCheckpoints, restartAttempts,
                    restartDelaySeconds, transactionTimeoutMs);
        }
    }
}
