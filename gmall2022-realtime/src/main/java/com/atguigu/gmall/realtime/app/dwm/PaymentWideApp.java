package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class PaymentWideApp {

    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取支付表与订单宽表流数据
        String paymentSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String groupId = "payment_wide_app";
        String sinkTopic = "dwm_payment_wide";

        DataStreamSource<String> paymentDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentSourceTopic, groupId));
        DataStreamSource<String> orderWideDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));

        // 3.转换数据 并提取watemark事件时间
        //id,out_trade_no,order_id,user_id,payment_type,trade_no,total_amount,subject,payment_status,create_time,callback_time,callback_content
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWKDS = paymentDS.map(line -> JSON.parseObject(line, PaymentInfo.class)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                                return DateTimeUtil.toTs(paymentInfo.getCreate_time());
                            }
                        })

        );

        SingleOutputStreamOperator<OrderWide> orderWideWithWKDS = orderWideDS.map(line -> JSON.parseObject(line, OrderWide.class)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide orderWide, long l) {
                                return DateTimeUtil.toTs(orderWide.getCreate_time());
                            }
                        })
        );


        // 4.分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyByDS = paymentInfoWithWKDS.keyBy(line -> line.getOrder_id());
        KeyedStream<OrderWide, Long> orderWideKeyByDS =   orderWideWithWKDS.keyBy(line -> line.getOrder_id());

        // 5.双流join
        SingleOutputStreamOperator<PaymentWide> joinDS = paymentInfoKeyByDS.intervalJoin(orderWideKeyByDS)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> collector) throws Exception {
                        collector.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        // 6.打印数据并写入kafka
        joinDS.print();
        joinDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        // 7.执行程序
        env.execute("PaymentWideApp");



    }

}
