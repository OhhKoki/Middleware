package com.example;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class customDebeziumDS implements DebeziumDeserializationSchema<String> {
    /**
     * 反序列化
     * @param sourceRecord
     * @param collector
     * @throws Exception
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 1.数据快照
        JSONObject snapshot = getSnapshot(sourceRecord);
        // 2.输出结果
        collector.collect(snapshot.toString());
    }
    /**
     * 数据类型
     * @return
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
    /**
     * 获取数据快照
     * @param sourceRecord
     * @return
     */
    private JSONObject getSnapshot(SourceRecord sourceRecord) {
        // 数据快照
        JSONObject result = new JSONObject();
        // 1.获取操作的 database 与 table 名称
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String dataBaseName = split[1];
        String tableName = split[2];
        // 2.获取 SQL 操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        // 3.获取变更前后的数据
        Struct value = (Struct) sourceRecord.value();
        // 变更前的数据
        Struct beforeStruct = value.getStruct("before");
        // 变更后的数据
        Struct afterStruct = value.getStruct("after");
        // 自定义操作最终返回的数据：变更前的数据
        JSONObject beforeJSONData = new JSONObject();
        // 自定义操作最终返回的数据：变更后的数据
        JSONObject afterJSONData = new JSONObject();
        if (null != beforeStruct && null != afterStruct) {    // update 操作
            // update 前的数据
            Schema beforeSchema = beforeStruct.schema();
            for (Field field : beforeSchema.fields()) {
                beforeJSONData.put(field.name(), beforeStruct.get(field));
            }
            // update 后的数据
            Schema afterSchema = afterStruct.schema();
            for (Field field : afterSchema.fields()) {
                afterJSONData.put(field.name(), afterStruct.get(field));
            }
        }else if (null != afterStruct) {    // insert 操作
            Schema afterSchema = afterStruct.schema();
            for (Field field : afterSchema.fields()) {
                afterJSONData.put(field.name(), afterStruct.get(field));
            }
        }else if (null != beforeStruct) {    // 删除操作
            Schema beforeSchema = beforeStruct.schema();
            for (Field field : beforeSchema.fields()) {
                beforeJSONData.put(field.name(), beforeStruct.get(field));
            }
        }else {
            System.out.println("异常情况");
        }
        // 4. 封装数据快照
        result.put("dataBaseName", dataBaseName);
        result.put("tableName", tableName);
        result.put("operation", operation.toString().toLowerCase());
        result.put("beforeJSONData", beforeJSONData);
        result.put("afterJSONData", afterJSONData);
        return result;
    }
}