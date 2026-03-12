package com.zhonghe.flink.cdc.collector;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
 * 简化配置，连接的是老实例（不支持事物和幂等）
 *
 * @Author: diweikang
 * @Date: 2026/3/9 17:06
 * @Description:
 */
public class CdcCollectorJob2 {

    private static final Logger log = LoggerFactory.getLogger(CdcCollectorJob2.class);

    // 配置参数（生产环境应从外部传入）
    private static final String MYSQL_HOST = "10.0.6.111";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_USERNAME = "aliadb";
    private static final String MYSQL_PASSWORD = "V8%jbQtsR&*W";
    private static final String MYSQL_DATABASE = "test_crm_db";
    private static final String[] MYSQL_TABLES = {
            "test_crm_db.crm_student",
            "test_crm_db.crm_clue_extend"
    };

    private static final String KAFKA_BOOTSTRAP_SERVERS =
            "alikafka-pre-cn-20s3w9g3e005-1-vpc.alikafka.aliyuncs.com:9092," +
                    "alikafka-pre-cn-20s3w9g3e005-2-vpc.alikafka.aliyuncs.com:9092," +
                    "alikafka-pre-cn-20s3w9g3e005-3-vpc.alikafka.aliyuncs.com:9092";

    private static final String KAFKA_TOPIC = "crm-cdc-events-topic-test";

    // 并行度配置
    private static final int PARALLELISM = 3;
    private static final String SERVER_ID_RANGE = "5400-5420"; // 必须 >= PARALLELISM

    public static void main(String[] args) throws Exception {
        System.out.println("启动CRM CDC采集作业...");
        System.out.println("MySQL: " + MYSQL_HOST + ":" + MYSQL_PORT + ", database=" + MYSQL_DATABASE);
        System.out.println("Kafka: " + KAFKA_BOOTSTRAP_SERVERS + ", topic=" + KAFKA_TOPIC);
        System.out.println("并行度: " + PARALLELISM + ", server-id范围: " + SERVER_ID_RANGE);

        // 1. 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.1 开启Checkpoint，设置EXACTLY_ONCE模式
        env.enableCheckpointing(60000); // 1分钟一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(600000); // 10分钟超时
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000); // 最小间隔30秒
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 并发数

        // 2 设置并行度
        env.setParallelism(PARALLELISM);

        // 3. 创建MySQL CDC Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(MYSQL_HOST)
                .port(MYSQL_PORT)
                .databaseList(MYSQL_DATABASE)
                .tableList(MYSQL_TABLES) // 指定监听的表
                .username(MYSQL_USERNAME)
                .password(MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 使用内置JSON反序列化器，保持原始格式
                .serverId(SERVER_ID_RANGE)
                .startupOptions(StartupOptions.latest()) // 或initial()
                .build();

        // 4. 添加Source
        DataStreamSource<String> cdcStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "mysql-cdc-source"
        );

        // 5. 打印原始数据（用于调试）
        cdcStream.map(json -> {
            System.out.println("【调试】收到原始数据: " + json);
            return json;
        });

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
                        return;
                    }

                    // 从source中获取表名
                    String table = source.getString("table");

                    // 提取业务ID
                    String businessKey = BusinessKeyExtractor.extractBusinessKey(table, value);

                    // 输出到下游
                    out.collect(new KafkaMessage(businessKey, value));

                    System.out.println("处理消息: key=" + businessKey + ", table=" + table);

                } catch (Exception e) {
                    log.error("处理消息失败: {}", value, e);
                }
            }
        });

        // 7. Kafka生产者配置（关键：保证发送顺序和Exactly-Once配置）
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);

        // Kafka生产者Exactly-Once配置
        //kafkaProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");      // 幂等性
        kafkaProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");                     // 所有副本确认
        kafkaProps.setProperty(ProducerConfig.RETRIES_CONFIG, "5");                    // 重试次数
        kafkaProps.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1"); // 关键：防止重试乱序

        // 事务配置（必须与broker匹配）
        //kafkaProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "cdc-transaction-"); // 让Flink自动管理事务ID
        //kafkaProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000"); // 事务超时配置

        // 8. 创建Kafka Sink
        KafkaSink<KafkaMessage> kafkaSink = KafkaSink.<KafkaMessage>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<KafkaMessage>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(
                            KafkaMessage message,
                            KafkaSinkContext context,
                            Long timestamp) {

                        return new ProducerRecord<>(
                                KAFKA_TOPIC,
                                message.key.getBytes(StandardCharsets.UTF_8),      // 业务ID作为key
                                message.value.getBytes(StandardCharsets.UTF_8)     // 原始CDC数据作为value
                        );
                    }
                })
                .setKafkaProducerConfig(kafkaProps)
                .setDeliveryGuarantee(DeliveryGuarantee.NONE)
                .build();

        // 9. 添加Sink
        kafkaStream.sinkTo(kafkaSink);

        // 10. 添加日志监控（可选）
        kafkaStream.map(message -> {
            System.out.println("📤 发送消息: key=" + message.key + ", size=" + message.value.length() + " bytes");
            return message;
        });

        // 11. 执行作业
        System.out.println("启动Flink作业...");
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
