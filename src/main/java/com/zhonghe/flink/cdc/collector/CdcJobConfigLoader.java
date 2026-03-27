package com.zhonghe.flink.cdc.collector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * 负责 profile 解析和 yml 配置加载。
 */
public final class CdcJobConfigLoader {

    private CdcJobConfigLoader() {
    }

    public static String resolveProfile() {
        String profile = System.getProperty("profile");
        if (profile == null || profile.trim().isEmpty()) {
            profile = System.getenv("APP_PROFILE");
        }
        if (profile == null || profile.trim().isEmpty()) {
            profile = "test";
        }
        return profile.trim().toLowerCase();
    }

    public static CdcJobConfig loadConfig(String profile) {
        String resourceName = "application-" + profile + ".yml";
        try (InputStream in = CdcJobConfigLoader.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IllegalArgumentException("未找到配置文件: " + resourceName);
            }
            Map<String, String> kv = parseSimpleYaml(in);
            return CdcJobConfig.from(kv, resourceName);
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

    public static final class CdcJobConfig {
        public final String mysqlHost;
        public final int mysqlPort;
        public final String mysqlUsername;
        public final String mysqlPassword;
        public final String mysqlDatabase;
        public final String[] mysqlTables;
        public final String kafkaBootstrapServers;
        public final String kafkaTopic;
        public final int parallelism;
        public final String serverIdRange;
        public final long checkpointIntervalMs;
        public final long checkpointTimeoutMs;
        public final long minPauseBetweenCheckpointsMs;
        public final int maxConcurrentCheckpoints;
        public final int restartAttempts;
        public final int restartDelaySeconds;
        public final long transactionTimeoutMs;

        private CdcJobConfig(String mysqlHost, int mysqlPort, String mysqlUsername, String mysqlPassword,
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

        private static CdcJobConfig from(Map<String, String> kv, String source) {
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
            return new CdcJobConfig(mysqlHost, mysqlPort, mysqlUsername, mysqlPassword, mysqlDatabase, mysqlTables,
                    kafkaBootstrapServers, kafkaTopic, parallelism, serverIdRange, checkpointIntervalMs,
                    checkpointTimeoutMs, minPauseBetweenCheckpointsMs, maxConcurrentCheckpoints, restartAttempts,
                    restartDelaySeconds, transactionTimeoutMs);
        }
    }
}

