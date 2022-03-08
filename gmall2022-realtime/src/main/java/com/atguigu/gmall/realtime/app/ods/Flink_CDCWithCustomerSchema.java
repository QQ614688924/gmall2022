package com.atguigu.gmall.realtime.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.fucs.CustomDebeziumDeserializationSchema;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
                .databaseList("gmall")
                .startupOptions(StartupOptions.latest())
                .deserializer(new CustomDebeziumDeserializationSchema()).build();

//        4.使用CDCsource从Mysql读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

        mysqlDS.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));

        env.execute();

    }

}
