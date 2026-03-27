package com.zhonghe.flink.cdc.collector;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
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
        String profile = CdcJobConfigLoader.resolveProfile();
        CdcJobConfigLoader.CdcJobConfig config = CdcJobConfigLoader.loadConfig(profile);

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
}
