package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.google.inject.internal.cglib.core.$DuplicatesPredicate;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import ru.yandex.clickhouse.ClickHouseUtil;
import scala.Tuple4;

import java.util.Date;
import java.time.Duration;

// mock -> logger.sh -> kafka -> baseLogApp -> kafka ->  uv/ujApp -> Kafka -> vsApp
public class VisitorStatsApp {

    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //检查点CK相关设置
//        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        StateBackend fsStateBackend = new FsStateBackend(
//                "hdfs://hadoop102:8020/gmall/flink/checkpoint/VisitorStatsApp");
//        env.setStateBackend(fsStateBackend);
//        System.setProperty("HADOOP_USER_NAME","atguigu");

        // 2.读取pv，uj，sv的数据流

        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_uv_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app_031611";

        // pvDS -> pv 进入页面数  连续访问时长
        DataStreamSource<String> pvSource = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        // uvDS -> uv
        DataStreamSource<String> uvSource = env.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        // ujDS -> 跳出数
        DataStreamSource<String> ujSource = env.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));


        // 3.转换数据流成JSON对象
        SingleOutputStreamOperator<VisitorStats> pvDS = pvSource.map(line ->
        {
            JSONObject json = JSON.parseObject(line);
            JSONObject data = json.getJSONObject("common");
            JSONObject page = json.getJSONObject("page");
            String last_page_id = page.getString("last_page_id");


            Long sv = 0L;
            if (last_page_id == null || last_page_id.length() <= 0) {
                sv = 1L;
            }


            return new VisitorStats("",
                    "",
                    data.getString("vc"),
                    data.getString("ch"),
                    data.getString("ar"),
                    data.getString("is_new"),
                    0L,
                    1L,
                    sv,
                    0L,
                    page.getLong("during_time"),
                    json.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> uvDS = uvSource.map(line ->
        {
            JSONObject json = JSON.parseObject(line);
            JSONObject data = json.getJSONObject("common");

            return new VisitorStats("",
                    "",
                    data.getString("vc"),
                    data.getString("ch"),
                    data.getString("ar"),
                    data.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    json.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> ujDS = ujSource.map(line -> {
            JSONObject json = JSON.parseObject(line);
            JSONObject data = json.getJSONObject("common");

            return new VisitorStats("",
                    "",
                    data.getString("vc"),
                    data.getString("ch"),
                    data.getString("ar"),
                    data.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    json.getLong("ts"));
        });
//        ujDS.print();
        // 4.union流
        DataStream<VisitorStats> unionDS = pvDS.union(uvDS, ujDS);

        // 5.提取watewark水位线时间
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats visitorStats, long l) {
                        return visitorStats.getTs();
                    }
                }));

        // 6.根据4个维度分组   渠道，版本，地区，新老客
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyByDS = visitorStatsWithDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats vs) throws Exception {
                return new Tuple4<>(vs.getVc(), vs.getCh(), vs.getAr(), vs.getIs_new());
            }
        });

        // 7.开窗聚合数据，同时补充开始、结束时间
        // 开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyByDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats v1, VisitorStats v2) throws Exception {

                v1.setPv_ct(v1.getPv_ct() + v2.getPv_ct());
                v1.setUv_ct(v1.getUv_ct() + v2.getUv_ct());
                v1.setUj_ct(v1.getUj_ct() + v2.getUj_ct());
                v1.setSv_ct(v1.getSv_ct() + v2.getSv_ct());
                v1.setDur_sum(v1.getDur_sum() + v2.getDur_sum());

                return v1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {
                //填充TS
                long start = timeWindow.getStart();
                long end = timeWindow.getEnd();
                VisitorStats visitorStats = iterable.iterator().next();

                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));

                collector.collect(visitorStats);

            }
        });
        reduceDS.print();
        // 8.写入ClickHouse
        reduceDS.addSink(MyClickHouseUtil.getSink("insert into visitor_stats_2022 values(?,?,?,?,?,?,?,?,?,?,?,?)"));
        // 9. 执行程序
        env.execute("VisitorStatsApp");


    }

}
