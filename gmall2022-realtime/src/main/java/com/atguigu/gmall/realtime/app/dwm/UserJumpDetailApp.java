package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//数据流  app/web -> nginx -> springboot ->kafka(ods) -> flink -> kafka(dwd) -> flink -> dwm
//程序流  mock ->nginx -> logger.sh  -> kafka(ods)  -> baseLogApp -> Kafka dwd  -> UniqueVisitorApp -> dwm
public class UserJumpDetailApp {

    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 读取kafka数据
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

        // 3. 将每行数据转行成json对象，并且设置watermark事件
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject).assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }));

        // 4. 定义序列模式
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new IterativeCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).next("next").where(new IterativeCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).within(Time.seconds(10));

        // 5. 将序列模式定义到流上
        // 5.1 分组  按照用户来算跳出率
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(line -> line.getJSONObject("common").getString("mid"));
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // 6. 提取匹配时间和超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeOutTag") {
        };
        SingleOutputStreamOperator<JSONObject> patternDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0);
            }
        });

        DataStream<JSONObject> timeOutDS = patternDS.getSideOutput(timeOutTag);

        // 7. union流
        DataStream<JSONObject> unionDS = patternDS.union(timeOutDS);

        // 8. 输出到kafka
        unionDS.print();
        unionDS.map(line -> line.toJSONString()).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        // 9. 执行程序

        env.execute("UserJumpDetailApp");


    }

}
