package com.zhonghe.flink.cdc.collector;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Kafka 连接测试
 *
 * @Author: diweikang
 * @Date: 2026/3/10 11:54
 * @Description:
 */
public class KafkaConnectionTest {
    public static void main(String[] args) {
        // 1. Kafka 集群地址
        String bootstrapServers = "alikafka-post-cn-n1e4ounyr002-1-vpc.alikafka.aliyuncs.com:9092," +
                "alikafka-post-cn-n1e4ounyr002-2-vpc.alikafka.aliyuncs.com:9092," +
                "alikafka-post-cn-n1e4ounyr002-3-vpc.alikafka.aliyuncs.com:9092";
        String topic = "crm-cdc-events-topic-test";

        // 2. 生产者配置（最简单的配置）
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 3. 发送测试消息
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "test-key", "{\"test\":\"data\"}");
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println("发送成功: partition=" + metadata.partition() +
                    ", offset=" + metadata.offset());
        } catch (Exception e) {
            System.err.println("发送失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
