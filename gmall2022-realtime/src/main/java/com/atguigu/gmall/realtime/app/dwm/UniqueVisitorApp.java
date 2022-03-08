package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

//数据流  app/web -> nginx -> springboot ->kafka(ods) -> flink -> kafka(dwd) -> flink -> dwm
//程序流  mock ->nginx -> logger.sh  -> kafka(ods)  -> baseLogApp -> Kafka dwd  -> UniqueVisitorApp -> dwm

public class UniqueVisitorApp {

    public static void main(String[] args) throws Exception {

        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //与kafka分区数保持一致

        // 2.消费kafka数据
        String topic = "dwd_page_log";
        String groupId = "dwd_page_log_uv_app";
        String sinkTopic = "dwm_uv_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));

        // 3.将每行数据转换成json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        // 4.按照mid分组，然后过滤数据

        KeyedStream<JSONObject, String> keyByDS = jsonObjDS.keyBy(line -> line.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> filterDS= keyByDS.filter(new RichFilterFunction<JSONObject>() {
            ValueState<String> valueState ;
            SimpleDateFormat simpleDateFormat;



            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> dtState = new ValueStateDescriptor<>("dt_state", String.class);
                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                //设置状态超时时间，防止状态过大
                dtState.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(dtState);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                // 1.last_page_id 为空的时候，更新状态
                if (lastPageId == null || lastPageId.length() <=0) {
                    Long ts = jsonObject.getLong("ts");
                    String curDt = simpleDateFormat.format(ts);
                    String dtState = valueState.value();

                    if(!curDt.equals(dtState) || null == dtState){
                        // 2.如果不为空，判断 时间是否为同一天， 如果不是更新状态,并返回true
                        valueState.update(curDt);
                        return true;
                    }

                }
                //3. 否则过滤掉
                return false;
            }
        });

        // 5.将数据输出至kafka
        filterDS.print("uv>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        filterDS.map(line->line.toJSONString()).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        // 6.执行程序
        env.execute("UniqueVisitorApp");


    }


}
