package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.fucs.SplitFunction;
import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.utils.MyClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {

    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.创建表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";

        String ddl = "create table page_view ( " +
                "common Map<string,string>, " +
                "page Map<string,string>, " +
                "ts bigint, " +
                "rt as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "watermark for rt as rt - interval '1' second " +
                ") " + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId);

        System.out.println(ddl);
        tableEnv.executeSql(ddl);

        // 3.过滤数据  last_page_id = search and item 不为空
        Table fullTable = tableEnv.sqlQuery("" +
                "select  " +
                "    page['item'] as full_word, " +
                "    rt " +
                "from " +
                "   page_view  " +
                "where page['last_page_id'] = 'search' and page['item'] is not null ");
        // 4.注册自定义函数，分词
        tableEnv.createTemporarySystemFunction("split_word", SplitFunction.class);

        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT  " +
                "    word,   " +
                "    rt  " +
                "FROM  " + fullTable + " , LATERAL TABLE(split_word(full_word))");

        // 5.分组，开窗，聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select   " +
                "    date_format(TUMBLE_START(rt,interval '10' second),'yyyy-MM-dd HH:mm:ss') stt,  " +
                "    date_format(TUMBLE_END(rt,interval '10' second),'yyyy-MM-dd HH:mm:ss') edt,  " +
                "    word as keyword,  " +
                "    count(*) ct,  " +
                "    UNIX_TIMESTAMP()*1000 ts,  " +
                "    'search ' as source  " +
                "from    " +
                "    "+ splitTable +"   " +
                "group by   " +
                "    word,  " +
                "    TUMBLE(rt , interval '10' second)");
        // 6.将表转换成流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);
        keywordStatsDataStream.print();

        // 7.打印并写入clickhouse
        keywordStatsDataStream.addSink(MyClickHouseUtil.getSink("insert into  keyword_stats_2022  (keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        // 8.执行程序
        env.execute();


    }

}
