package com.atguigu.gmall.realtime.app.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class Flink_CDCWithCustomerSchema {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.创建Flink-Mysql-CDC的source
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .startupOptions(StartupOptions.latest())
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception { //3.自定义数据解析器
                        //获取主题信息,包含着数据库和表名  mysql_binlog_source.gmall-flink.z_user_info
                        String topic = sourceRecord.topic();
                        String[] arr = topic.split("\\.");
                        String db = arr[1];
                        String tableName = arr[2];


                        //获取操作类型 READ DELETE UPDATE CREATE
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

                        //获取值信息并转换为Struct类型
                        Struct value = (Struct) sourceRecord.value();

                        //获取变化后的数据
                        Struct after = value.getStruct("after");
                        System.out.println(after);
                        //创建JSON对象用于存储数据信息
                        JSONObject data = new JSONObject();
                        if (after != null) {
                            Schema schema = after.schema();
                            for (Field field : schema.fields()) {
                                data.put(field.name(), after.get(field.name()));
                            }
                        }

                        //创建JSON对象用于封装最终返回值数据信息
                        JSONObject res = new JSONObject();

                        res.put("operation", operation.toString().toLowerCase());
                        res.put("data", data);
                        res.put("database", db);
                        res.put("table", tableName);

                        //发送数据至下游
                        collector.collect(res.toJSONString());

                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                }).build();

//        4.使用CDCsource从Mysql读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

        mysqlDS.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));

        env.execute();

    }

}
