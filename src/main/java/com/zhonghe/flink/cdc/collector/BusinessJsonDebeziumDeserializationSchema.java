package com.zhonghe.flink.cdc.collector;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * 将 Debezium SourceRecord 转成“下游可直接消费”的业务 JSON：
 * - 保留 before/after/source/op/ts_ms
 * - 提升 database/table 到顶层
 * - Decimal 逻辑类型统一转为可读数字字符串，避免 base64 形态
 */
public class BusinessJsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    private static final long serialVersionUID = 1L;
    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_BEFORE = "before";
    private static final String FIELD_AFTER = "after";
    private static final String FIELD_TS_MS = "ts_ms";
    private static final String FIELD_OP = "op";

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) {
        Object valueObj = record.value();
        if (!(valueObj instanceof Struct)) {
            out.collect(String.valueOf(valueObj));
            return;
        }

        Struct value = (Struct) valueObj;
        Struct source = value.getStruct(FIELD_SOURCE);
        Struct before = value.getStruct(FIELD_BEFORE);
        Struct after = value.getStruct(FIELD_AFTER);

        JSONObject event = new JSONObject();
        event.put(FIELD_OP, extractOperation(record, value));
        event.put(FIELD_TS_MS, value.getInt64(FIELD_TS_MS));
        event.put(FIELD_SOURCE, source == null ? null : (JSONObject) convertValue(source, source.schema()));
        event.put(FIELD_BEFORE, before == null ? null : (JSONObject) convertValue(before, before.schema()));
        event.put(FIELD_AFTER, after == null ? null : (JSONObject) convertValue(after, after.schema()));

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
                if (!(value instanceof Struct)) {
                    return value;
                }
                Struct struct = (Struct) value;
                JSONObject obj = new JSONObject();
                for (Field field : schema.fields()) {
                    Object fieldValue = struct.get(field);
                    obj.put(field.name(), convertValue(fieldValue, field.schema()));
                }
                return obj;
            case ARRAY:
                JSONArray arr = new JSONArray();
                if (!(value instanceof List)) {
                    return arr;
                }
                List<Object> list = (List<Object>) value;
                for (Object item : list) {
                    arr.add(convertValue(item, schema.valueSchema()));
                }
                return arr;
            case MAP:
                JSONObject mapObj = new JSONObject();
                if (!(value instanceof Map)) {
                    return mapObj;
                }
                Map<Object, Object> map = (Map<Object, Object>) value;
                for (Map.Entry<Object, Object> entry : map.entrySet()) {
                    String key = String.valueOf(entry.getKey());
                    mapObj.put(key, convertValue(entry.getValue(), schema.valueSchema()));
                }
                return mapObj;
            case BYTES:
                byte[] bytes = toBytes(value);
                return bytes == null ? null : BASE64_ENCODER.encodeToString(bytes);
            default:
                return value;
        }
    }

    private byte[] toBytes(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof java.nio.ByteBuffer) {
            java.nio.ByteBuffer buf = ((java.nio.ByteBuffer) value).duplicate();
            byte[] out = new byte[buf.remaining()];
            buf.get(out);
            return out;
        }
        return null;
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
        if (value instanceof java.nio.ByteBuffer) {
            byte[] bytes = toBytes(value);
            return bytes == null ? null : Decimal.toLogical(schema, bytes);
        }
        return new BigDecimal(value.toString());
    }

    private String extractOperation(SourceRecord record, Struct value) {
        try {
            return Envelope.operationFor(record).code();
        } catch (Exception e) {
            Object op = value.get(FIELD_OP);
            return op == null ? "unknown" : String.valueOf(op);
        }
    }
}

