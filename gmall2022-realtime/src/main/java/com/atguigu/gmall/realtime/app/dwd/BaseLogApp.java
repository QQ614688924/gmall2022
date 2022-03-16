package com.atguigu.gmall.realtime.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

//数据流  app/web -> nginx -> springboot ->kafka(ods) -> flink -> kafka(dwd)
//程序流  mock ->nginx -> logger.sh  -> kafka(ods)  -> baseLogApp -> Kafka dwd

public class BaseLogApp {

    public static void main(String[] args) throws Exception {
        //TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        //TODO 2.设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall_flink/dwd_log/ck"));

        //TODO 3.设置检查点
//        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 4.消费Kafka数据
        String topic = "ods_base_log";
        String groupId = "ods_base_log_gmall_031611";
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));

        //TODO 5.将数据转换成JSON对象
        //为什么不用map进行转换？   因为json可能存在脏数据，使用map会导致程序挂掉
        OutputTag<String> drityTag = new OutputTag<String>("Drity") {};
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {


                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    //脏数据，输出到测输出流
                    context.output(drityTag, s);
                }

            }
        });
//        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaSource.map(JSON :: parseObject);
//
//        jsonDS.print();
        //TODO 6.识别新老客用户，状态编程
        //6.1 按照设备id去识别新老用户，所以得先按照设备id进行分组

        KeyedStream<JSONObject, Object> keyByDS = jsonDS.keyBy(line -> line.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> mapDS = keyByDS.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("isNew", String.class));
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {

                String isNew = jsonObject.getJSONObject("common").getString("is_new");

                if ("1".equals(isNew)) {

                    String value = valueState.value();
                    Long ts = jsonObject.getLong("ts");
                    if (value != null) { //如果状态不为空 修复数据
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    } else {
                        //如果为空 更新状态
                        valueState.update(simpleDateFormat.format(ts));
                    }

                }

                return jsonObject;
            }
        });

        //TODO 7.利用侧输出流进行分流，将启动、曝光日志输出至测输出流 ，也买你日志输出至主流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS = mapDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    context.output(startTag, jsonObject.toJSONString());
                } else {

                    collector.collect(jsonObject.toJSONString());
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);

                            String page_id = jsonObject.getJSONObject("page").getString("page_id");

                            display.put("page_id", page_id);

                            context.output(displayTag, display.toJSONString());

                        }

                    }

                }
            }
        });

        //TODO 8.打印流，并发送至Kafka
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> drityDS = jsonDS.getSideOutput(drityTag);
        drityDS.print("drity>>>>>>>>>>>>>>>>>>>>>>");
        startDS.print("start>>>>>>>>>>>>>>>>>>>>>>>");
        displayDS.print("display>>>>>>>>>>>>>>>>>>>");
        pageDS.print("pageDS>>>>>>>>>>>>>>>>>>>>>>>>");

        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        //TODO 9.执行程序
        env.execute();


    }

}
