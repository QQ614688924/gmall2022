package com.atguigu.gmall.realtime.app.fucs;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

//        //获取主题  解析数据库和表名
//        String topic = sourceRecord.topic();
//        String[] arr = topic.split("\\.");
//        String db = arr[1];
//        String tableName = arr[2];
//
//        //获取操作类型
//
//
//        Struct value = (Struct) sourceRecord.value();
//
//        Struct before = value.getStruct("before");
//
//        JSONObject before_data = new JSONObject();
//
//        if (before != null) {
//            Schema schema = before.schema();
//            for (Field field : schema.fields()) {
//                before_data.put(field.name(), before.get(field.name()));
//            }
//        }
//
//        Struct after = value.getStruct("after");
//
//        JSONObject after_data = new JSONObject();
//
//        if (after != null) {
//            Schema schema = after.schema();
//            for (Field field : schema.fields()) {
//                after_data.put(field.name(), after.get(field.name()));
//            }
//        }
//        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
//
//        JSONObject res = new JSONObject();
//
//        res.put("database",db);
//        res.put("table",tableName);
//        res.put("before",before_data);
//        res.put("after",after_data);
//        res.put("operation",operation.toString().toLowerCase());
//        System.out.println(res.toJSONString());
//
//        collector.collect(res.toJSONString());

        //1.创建JSON对象用于存储最终数据
        JSONObject result = new JSONObject();

        //2.获取库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        Struct value = (Struct) sourceRecord.value();
        //3.获取"before"数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            for (Field field : beforeFields) {
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(), beforeValue);
            }
        }

        //4.获取"after"数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for (Field field : afterFields) {

                Object afterValue = after.get(field.name());
//                System.out.println(tableName + "========" + field + ">>>>>>>>>" + afterValue);
                afterJson.put(field.name(), afterValue);
            }
        }

        //5.获取操作类型  CREATE UPDATE DELETE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        //6.将字段写入JSON对象
        result.put("database", database);
        result.put("table", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("operation", type);
//        System.out.println(result.toJSONString());

        //7.输出数据
        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
