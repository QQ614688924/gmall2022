package com.atguigu.gmall.realtime.app.fucs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;
    private OutputTag<JSONObject> outputTag;
    private MapStateDescriptor<String,TableProcess> mapStateDescriptor;
    private ValueState<TableProcess> tableState;

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

//        connection.setAutoCommit(true);
    }

    //value:{"db":"","tn":"","before":{},"after":{},"type":""}
    @Override
    public void processBroadcastElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        // 1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");


        //转换对象
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        // 2.检验表是否存在,并创建语句
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkExtend());
        }


        // 3.写入状态，广播出去
        //获取广播流对象
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + ":" + tableProcess.getOperateType();
//        System.out.println("写入key为："+key);
        broadcastState.put(key,tableProcess);

    }

    private void checkTable(String sinkTable, String sinkPk, String sinkColumns, String sinkExtend) {
        // create table if not exists db.xx ( id varchar primary_key, name varchar)
        PreparedStatement preparedStatement = null;

        try {
            if (sinkPk == null) sinkPk = "id";
            if (sinkExtend == null) sinkExtend = "";
            StringBuffer createTableSql = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");

            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if (sinkPk.equals(column)) {
                    createTableSql
                            .append(column)
                            .append(" varchar primary key ");
                }else{
                    createTableSql
                            .append(column)
                            .append(" varchar");
                }

                if (i < columns.length-1){
                    createTableSql.append(",");
                }

            }
            createTableSql.append(")");
            createTableSql.append(sinkExtend);
            System.out.println("建表语句" + createTableSql);
            preparedStatement = connection.prepareStatement(createTableSql.toString());

            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("创建表"+ sinkTable +"，创建失败！");
        }finally {
            if (preparedStatement !=null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


    }


    //value:{"db":"","tn":"","before":{},"after":{},"operation":""}
    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        // 1.读取状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

        String key = jsonObject.getString("table") + ":" + jsonObject.getString("operation");
        System.out.println("读取key："+key);
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            JSONObject data = jsonObject.getJSONObject("after");

            // 2.过滤数据
            filterColumns(data,tableProcess.getSinkColumns());

            // 3.分流
            //将输出表/主题信息写入Value
            jsonObject.put("sinkTable",tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            //Kafka数据,写入主流
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                collector.collect(jsonObject);
            }else{
                readOnlyContext.output(outputTag,jsonObject);
            }

            //HBase数据,写入侧输出流

        }else{
            System.out.println("该组合key不存在！");
        }




    }

    /**
     *
     * @param data   {"id":"11","tm_name":"atguigu","logo_url":"aaa"}
     * @param sinkColumns  {"id":"11","tm_name":"atguigu"}
     */

    // bug地方
    private void filterColumns(JSONObject data, String sinkColumns) {

        String[] fileds = sinkColumns.split(",");
        List<String> list = (List<String>) Arrays.asList(fileds);

        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();

        while (iterator.hasNext()) {

            Map.Entry<String, Object> next = iterator.next();

            if (!list.contains(next.getKey())) {
                iterator.remove();
            }

        }


    }

}
