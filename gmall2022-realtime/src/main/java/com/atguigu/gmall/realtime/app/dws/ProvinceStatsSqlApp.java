package com.atguigu.gmall.realtime.app.dws;


import com.atguigu.gmall.realtime.bean.ProvinceStats;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class ProvinceStatsSqlApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用DDL创建表 提取时间戳生成WaterMark
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        String ddl = "create table order_wide ( " +
                "`province_id` bigint," +
                "`province_name` string," +
                "`province_area_code` string," +
                "`province_iso_code` string," +
                "`province_3166_2_code` string," +
                "`order_id` bigint," +
                "`split_total_amount` decimal," +
                "`create_time` string," +
                "`rt` as to_timestamp(create_time)," +
                " watermark for rt as rt- interval '1' second ) ";


        tableEnv.executeSql(ddl+ MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId));

        //TODO 3.查询数据  分组、开窗、聚合
        String sql = "select " +
                "   date_format(TUMBLE_START(rt, INTERVAL '10' SECOND) ,'yyyy-MM-dd HH:mm:ss') as stt," +
                "   date_format(TUMBLE_END(rt, INTERVAL '10' SECOND) ,'yyyy-MM-dd HH:mm:ss') as edt," +
                "   province_id," +
                "   province_name," +
                "   province_area_code," +
                "   province_iso_code," +
                "   province_3166_2_code," +
                "   sum(split_total_amount) order_amount," +
                "   count(distinct order_id) order_count," +
                "   UNIX_TIMESTAMP()*1000 ts " +
                "from " +
                "   order_wide " +
                "group by " +
                "   province_id," +
                "   province_name," +
                "   province_area_code," +
                "   province_iso_code," +
                "   province_3166_2_code," +
                "   TUMBLE(rt, INTERVAL '10' SECOND)";
//        System.out.println(sql);

        Table table = tableEnv.sqlQuery(sql);

        //TODO 4.将动态表转换为流
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(table, ProvinceStats.class);

        //TODO 5.打印数据并写入ClickHouse
        provinceStatsDS.print();
        provinceStatsDS.addSink(MyClickHouseUtil.getSink("insert into province_stats_2022 values (?,?,?,?,?,?,?,?,?,?)"));

        //TODO 6.启动任务
        env.execute("ProvinceStatsSqlApp");

    }

}
