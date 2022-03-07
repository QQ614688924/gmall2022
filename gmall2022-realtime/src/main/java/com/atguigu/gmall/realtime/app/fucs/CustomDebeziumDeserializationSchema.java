package com.atguigu.gmall.realtime.app.fucs;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class CustomDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {

        //获取主题  解析数据库和表名
        String topic = sourceRecord.topic();
        String[] arr = topic.split("\\.");
        String db = arr[1];
        String tableName = arr[2];

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        Struct value = (Struct) sourceRecord.value();

        Struct before = value.getStruct("before");

        JSONObject before_data = new JSONObject();

        if (before != null) {
            Schema schema = before.schema();
            for (Field field : schema.fields()) {
                before_data.put(field.name(), before.get(field.name()));
            }
        }

        Struct after = value.getStruct("after");

        JSONObject after_data = new JSONObject();

        if (after != null) {
            Schema schema = after.schema();
            for (Field field : schema.fields()) {
                after_data.put(field.name(), after.get(field.name()));
            }
        }

        JSONObject res = new JSONObject();

        res.put("database",db);
        res.put("table",tableName);
        res.put("before",before_data);
        res.put("after",after_data);
        res.put("operation",operation);

        collector.collect(res);

    }

    @Override
    public TypeInformation getProducedType() {
        return null;
    }
}
