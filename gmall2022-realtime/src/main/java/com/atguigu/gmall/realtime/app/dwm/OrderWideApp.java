package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.fucs.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.google.inject.internal.cglib.core.$DuplicatesPredicate;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;


public class OrderWideApp {

    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读取kafka中order_info和order_detail的数据，并转成javabean，同时提取watewark时间
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group_031612";

        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId))
                .map(line -> {
                    OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);

                    String create_time = orderInfo.getCreate_time();
                    String[] dateTimeArr = create_time.split(" ");
                    orderInfo.setCreate_date(dateTimeArr[0]);
                    orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

                    return orderInfo;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        }));
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId))
                .map(line -> {
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderDetail.setCreate_ts(sdf.parse(create_time).getTime());

                    return orderDetail;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        }));

        //TODO 3.双流JOIN
        SingleOutputStreamOperator<OrderWide> orderWideWithNoDimDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5)) //生成环境中给的时间给最大延迟时间
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect( new OrderWide(orderInfo, orderDetail));
                    }
                });

        //打印测试
          orderWideWithNoDimDS.print("orderWideWithNoDimDS>>>>>>>>>");


        // 4.关联维度信息   异步I/O 的形式提高效率

        // 4.1关联用户维度信息   异步I/O 的形式提高效率
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideWithNoDimDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setUser_gender(dimInfo.getString("gender"));

                        String birthday = dimInfo.getString("birthday");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        long currentTs = System.currentTimeMillis();
                        long ts = sdf.parse(birthday).getTime();

                        long age = (currentTs - ts) / (1000 * 60 * 60 * 24 * 365L);

                        orderWide.setUser_age((int) age);
                    }
                },
                60,
                TimeUnit.SECONDS);
//        orderWideWithUserDS.print("orderWideWithUserDS>>>>>>>>>>");

//        //4.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setProvince_name(dimInfo.getString("name"));
                        orderWide.setProvince_area_code(dimInfo.getString("areaCode"));
                        orderWide.setProvince_iso_code(dimInfo.getString("isoCode"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("iso31662"));
                    }
                }, 60, TimeUnit.SECONDS);
//        orderWideWithProvinceDS.print("orderWideWithProvinceDS>>>>>>>>>>>>>>>>>>>>>>>>");
//        //4.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("skuName"));
                        orderWide.setCategory3_id(jsonObject.getLong("category3Id"));
                        orderWide.setSpu_id(jsonObject.getLong("spuId"));
                        orderWide.setTm_id(jsonObject.getLong("tmId"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);
//        orderWideWithSkuDS.print("orderWideWithSkuDS>>>>>>>>>>>>>>>>>>>");
////
//        //4.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("spuName"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);
//        orderWideWithSpuDS.print("orderWideWithSpuDS>>>>>>>>>>>>>>>>>");
//
//        //4.5 关联TM维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("tmName"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);
//
//        orderWideWithTmDS.print("orderWideWithTmDS>>>>>>>>>>>>>>");
//        //4.6 关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("name"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>>>>>>>>>");


        // 5.将宽表数据写入Kafka中
        orderWideWithCategory3DS.map(line -> JSON.toJSONString(line)).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        // 6.执行程序
        env.execute("OrderWideApp");


    }

}
