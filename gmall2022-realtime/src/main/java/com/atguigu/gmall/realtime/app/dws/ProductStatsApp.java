package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.fucs.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.bean.ProductStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取7个流的数据
        String groupId = "product_stats_app11";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorDS = env.addSource(MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId));
        DataStreamSource<String> cartDS = env.addSource(MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId));
        DataStreamSource<String> orderDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId));
        DataStreamSource<String> refundDS = env.addSource(MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentDS = env.addSource(MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId));

        //3.将7个流转换成统一格式的对象
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS =

                pvDS.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String line, Collector<ProductStats> collector) throws Exception {

                JSONObject jsonObject = JSON.parseObject(line);

                JSONObject page = jsonObject.getJSONObject("page");
                String page_id = page.getString("page_id");
                Long ts = jsonObject.getLong("ts");

                //点击次数
                if ("good_detail".equals(page_id) && "sku_id".equals(page.getString("item_type"))) {
                    ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build();
                }

                //曝光次数
                JSONArray displays = jsonObject.getJSONArray("displays");

                if (displays != null && displays.size() > 0) {

                    for (int i = 0; i < displays.size(); i++) {

                        JSONObject display = displays.getJSONObject(i);

                        if ("sku_id".equals(display.getString("item_type"))) {
                            ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build();
                        }
                    }
                }
            }
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorDS.map(line -> {
//            id,user_id,sku_id,spu_id,is_cancel,create_time,cancel_time
            JSONObject jsonObject = JSON.parseObject(line);
            String create_time = jsonObject.getString("create_time");

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(create_time))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(line -> {
//            id,user_id,sku_id,cart_price,sku_num,img_url,sku_name,is_checked,create_time,operate_time,is_ordered,order_time,source_type,source_id
            JSONObject jsonObject = JSON.parseObject(line);
            String create_time = jsonObject.getString("create_time");

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(create_time))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderDS.map(line ->
        {
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getSplit_total_amount())
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = paymentDS.map(line ->
        {
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getSplit_total_amount())
                    .paidOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();

        });

        productStatsWithPaymentDS.print("productStatsWithPaymentDS>>>>>>>>>>>>>>>>>>>");

        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(line -> {
            // id,user_id,order_id,sku_id,refund_type,refund_num,refund_amount,refund_reason_type,refund_reason_txt,refund_status,create_time
            JSONObject jsonObject = JSON.parseObject(line);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));


            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();

        });

        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(line ->
        {
            //id,user_id,nick_name,head_img,sku_id,spu_id,order_id,appraise,comment_txt,create_time,operate_time
            JSONObject jsonObject = JSON.parseObject(line);

            String appraise = jsonObject.getString("appraise");

            Long goodCt = 0L;

            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                goodCt = 1L;
            }


            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //4.将7个流合并在一起

        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(
                productStatsWithFavorDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS
        );

//        unionDS.print();

        //5.提取watewark
        SingleOutputStreamOperator<ProductStats> productStatsWithWKDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats productStats, long l) {
                        return productStats.getTs();
                    }
                })
        );

        //6.分组，聚合，开窗
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = productStatsWithWKDS.keyBy(ProductStats::getSku_id).window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(new ReduceFunction<ProductStats>() {
            @Override
            public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                //stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                //stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                //stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                return stats1;
            }
        }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
            @Override
            public void apply(Long aLong, TimeWindow timeWindow, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {

                // 提取数据
                ProductStats productStats = iterable.iterator().next();

                // 设置时间
                productStats.setStt(DateTimeUtil.toYMDhms(new Date(timeWindow.getStart())));
                productStats.setEdt(DateTimeUtil.toYMDhms(new Date(timeWindow.getEnd())));

                //设置个数
                productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                collector.collect(productStats);

            }
        });

        //7.补充信息

        //7.1 关联SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {

                        productStats.setSku_name(dimInfo.getString("skuName"));
                        productStats.setSku_price(dimInfo.getBigDecimal("price"));
                        productStats.setSpu_id(dimInfo.getLong("spuId"));
                        productStats.setTm_id(dimInfo.getLong("tmId"));
                        productStats.setCategory3_id(dimInfo.getLong("category3Id"));

                    }
                }, 60, TimeUnit.SECONDS);

        //7.2 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setSpu_name(jsonObject.getString("spuName"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //7.3 关联Category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setCategory3_name(jsonObject.getString("name"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //7.4 关联TM维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setTm_name(jsonObject.getString("tmName"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        productStatsWithTmDS.print("productStatsWithTmDS>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        //8.打印并写入clickhouse
        productStatsWithTmDS.addSink(MyClickHouseUtil.getSink("insert into table product_stats_2022 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //9.执行程序
        env.execute();

    }

}
