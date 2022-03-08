package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.fucs.DimSinkFunction;
import com.atguigu.gmall.realtime.app.fucs.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.app.fucs.CustomDebeziumDeserializationSchema;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDbApp {

    public static void main(String[] args) throws Exception {
        //TODO  1.创建执行环境，并设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO  2.设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall_flink/dwd_db/ck"));
//        env.enableCheckpointing(10000L,CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        String topic = "ods_base_db";
        String group_id = "base_db_app_gmall2022";
        //TODO  3.读取Kafka数据
        DataStreamSource<String> kafkaSourceDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, group_id));

        //TODO  4.将每行数据转成json对象 5.过滤数据
        SingleOutputStreamOperator<JSONObject> filterDS = kafkaSourceDS.map(line -> JSON.parseObject(line)).filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {

                String data = jsonObject.getString("data");
                return !(data != null && data.length() > 0);
            }
        });

        //TODO  6.cdc读取mysql配置表数据，形成广播流
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall_flink_realtime")
                .deserializer(new CustomDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

        MapStateDescriptor<String, TableProcess> mapState = new MapStateDescriptor<>("map_state", String.class, TableProcess.class);
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase_tag") {
        };
        BroadcastStream<String> broadcastStream = mysqlDS.broadcast(mapState);

        //TODO  7.主流连接广播流
        BroadcastConnectedStream<JSONObject, String> joinDS = filterDS.connect(broadcastStream);

        //TODO  8.分流，事实表数据输出至主流，维度表数据输出至侧输出流
        SingleOutputStreamOperator<JSONObject> kafkaDS = joinDS.process(new TableProcessFunction(hbaseTag, mapState));

        //TODO  9.分别打印和写入kafka以及hbase中
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);
        kafkaDS.print("kafka>>>>>>>>>>>>>>>>");
        hbaseDS.print("hbase>>>>>>>>>>>>>>>>");

        hbaseDS.addSink(new DimSinkFunction());
//        kafkaDS.addSink();
        kafkaDS.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sinkTable"),
                        jsonObject.getString("after").getBytes());
            }
        }));

        //TODO  10.执行程序
        env.execute("BaseDbApp");

    }

}
